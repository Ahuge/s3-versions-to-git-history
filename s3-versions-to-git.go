package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fatih/color"
	"github.com/go-git/go-git/v5/plumbing/object"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)
import "github.com/go-git/go-git/v5"

type gitContainer struct {
	Tree       *git.Worktree
	Repository *git.Repository
}

type S3Object struct {
	Key    string
	Bucket string
}

type S3VersionedObject struct {
	Key            string
	Bucket         string
	VersionId      string
	RepositoryRoot string
	LastModified   time.Time
}

func (svo *S3VersionedObject) toLocalPath() string {
	path := strings.Join([]string{svo.RepositoryRoot, svo.Bucket, svo.Key}, string(os.PathSeparator))

	absFilename, err := filepath.Abs(path)
	if err != nil {
		log.Printf("Couldn't create absolute path for %s\n", path)
		fmt.Println(err)
		return fmt.Sprintf("Error: %v", err)
	}
	return absFilename
}

func (svo *S3VersionedObject) toBasenamePath() string {
	return svo.Key
}

func errorMessage(message any) {
	color.Set(color.FgRed)
	fmt.Println(message)
	color.Unset()
}

func help() {
	helpMessage := "s3 Versions To Git History\n" +
		"\n" +
		"s3-versions-to-git-history --bucket=<s3Bucket> [--output=<outputDir>] [--profile=<awsProfile>] [--region=us-west-2]\n" +
		"\n" +
		"\t--bucket\t\tThe S3 bucket you'd like to turn into a git repo.\n" +
		"\t--output\t\tThe output directory to create a git repo in. Defaults to pwd if not provided.\n" +
		"\t--profile\t\tThe AWS Profile you'd like to use. Defaults to the \"default\" profile if not provided.\n" +
		"\t--region\t\tThe AWS Region you'd like to use. Defaults to us-west-2 if not provided.\n" +
		"\n"
	fmt.Printf(helpMessage)
}

func getS3Client(profile, region string) (*s3.Client, error) {
	var sdkConfig aws.Config
	var err error
	if profile != "" {
		sdkConfig, err = config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile(profile), config.WithRegion(region))
	} else {
		sdkConfig, err = config.LoadDefaultConfig(context.TODO())
	}

	if err != nil {
		log.Println("Couldn't load default configuration. Have you set up your AWS account?")
		errorMessage(err)
		return nil, err
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	return s3Client, nil
}

func queryS3Bucket(bucketName string, s3Client *s3.Client) ([]S3Object, error) {
	rawObjects := make([]S3Object, 0)

	result, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Printf("ListObjectsV2 failed while querying: %s\n", bucketName)
		errorMessage(err)
		return nil, err
	}
	contents := result.Contents
	for _, s3obj := range contents {
		rawObjects = append(rawObjects, S3Object{
			Key:    aws.ToString(s3obj.Key),
			Bucket: bucketName,
		})
	}
	return rawObjects, nil
}

func queryS3Versions(objects []S3Object, repoPath string, s3Client *s3.Client) ([]S3VersionedObject, error) {
	s3Objects := make([]S3VersionedObject, 0)
	for _, s3obj := range objects {
		result, err := s3Client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(s3obj.Bucket),
			Prefix: aws.String(s3obj.Key),
		})
		if err != nil {
			log.Println("Couldn't load default configuration. Have you set up your AWS account?")
			errorMessage(err)
			continue
		}
		for _, vers := range result.Versions {
			s3Objects = append(s3Objects, S3VersionedObject{
				Key:            aws.ToString(vers.Key),
				Bucket:         s3obj.Bucket,
				VersionId:      aws.ToString(vers.VersionId),
				LastModified:   aws.ToTime(vers.LastModified),
				RepositoryRoot: repoPath,
			})
		}
	}
	sort.Slice(s3Objects, func(i, j int) bool {
		return s3Objects[i].LastModified.Before(s3Objects[j].LastModified)
	})
	return s3Objects, nil
}

func replayS3Changes(versions []S3VersionedObject, s3Client *s3.Client, container gitContainer) {
	var currentDate time.Time
	objectModifications := make([]S3VersionedObject, 0)
	for _, version := range versions {
		if (currentDate == time.Time{}) {
			// Starting
			currentDate = version.LastModified
		}
		if version.LastModified.After(currentDate) {
			err := applyGitChanges(objectModifications, s3Client, container)
			if err != nil {
				log.Printf("Error applying Git changes for %d objects\n", len(objectModifications))
				errorMessage(err)
				return
			}
			objectModifications = make([]S3VersionedObject, 0)
			currentDate = version.LastModified
		}
		objectModifications = append(objectModifications, version)
		// log.Printf("Step %d: File: %s, Date: %s", i, version.Key, version.LastModified)
	}
}

func downloadFile(object S3VersionedObject, s3Client *s3.Client) error {
	result, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:    aws.String(object.Bucket),
		Key:       aws.String(object.Key),
		VersionId: aws.String(object.VersionId),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v@%v.\n", object.Bucket, object.Key, object.VersionId)
		errorMessage(err)
		return err
	}
	defer result.Body.Close()
	filename := object.toLocalPath()
	dirname := filepath.Dir(filename)
	_, err = os.Stat(dirname)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dirname, 0777)
		if err != nil {
			log.Printf("Couldn't create file %v.\n", filename)
			errorMessage(err)
			return err
		}
	}
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Couldn't create file %v.\n", filename)
		errorMessage(err)
		return err
	}
	defer file.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v@%v.\n", object.Key, object.VersionId)
		errorMessage(err)
	}
	_, err = file.Write(body)
	return err
}

func applyGitChanges(objects []S3VersionedObject, s3Client *s3.Client, container gitContainer) error {
	var commitDate time.Time
	files := make([]string, 0)
	for _, object := range objects {
		err := downloadFile(object, s3Client)
		if err != nil {
			log.Printf("Error downloading object %s:%s@%s to %s\n", object.Bucket, object.Key, object.VersionId, object.toLocalPath())
			errorMessage(err)
			return err
		}
		files = append(files, object.toLocalPath())
		commitDate = object.LastModified
		_, err = container.Tree.Add(object.toBasenamePath())
		if err != nil {
			log.Printf("Error staging object %s\n", object.toLocalPath())
			errorMessage(err)
			return err
		}
	}
	commitMsg := fmt.Sprintf("Modification on %s", commitDate)
	commit, err := container.Tree.Commit(commitMsg, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "s3-versions-to-git",
			Email: "ahughesalex@gmail.com",
			When:  commitDate,
		},
	})
	if err != nil {
		log.Printf("Error comitting objects to stage\n")
		errorMessage(err)
		return err
	}
	_, err = container.Repository.CommitObject(commit)
	if err != nil {
		log.Printf("Error comitting objects to repo\n")
		errorMessage(err)
		return err
	}

	log.Printf("Successfully applied commit with the following files:\n%s\n\t", strings.Join(files, "\n\t"))
	return nil
}

func main() {
	bucketFlag := flag.String("bucket", "", "The S3 bucket you'd like to turn into a git repo")
	outputFlag := flag.String("output", "", "Output directory to create git repo in")
	profileFlag := flag.String("profile", "", "The AWS Profile you'd like to use")
	regionFlag := flag.String("region", "us-west-2", "The AWS Region you'd like to use")
	helpFlag := flag.Bool("help", false, "Program usage and help")
	flag.Parse()
	if *helpFlag == true {
		help()
		return
	}
	if len(*bucketFlag) == 0 {
		help()
		errorMessage("Error: Please provide an S3 bucket")
		return
	}
	if len(*outputFlag) == 0 {
		abspath, err := filepath.Abs("")
		if err != nil {
			errorMessage("Error: Please provide an output directory")
			return
		}
		*outputFlag = abspath
	}

	repoPath := filepath.Join(*outputFlag, *bucketFlag)

	_, err := os.Stat(repoPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(repoPath, 0777)
		if err != nil {
			log.Printf("Unable to create directories %s\n", repoPath)
			errorMessage(err)
			return
		}
	}
	r, err := git.PlainOpen(repoPath)
	if err != nil {
		r, err = git.PlainInit(repoPath, false)
		if err != nil {
			log.Printf("Error unable to git init in the %s folder\n", repoPath)
			errorMessage(err)
			return
		}
	}
	worktree, err := r.Worktree()
	if err != nil {
		log.Printf("Error unable to initialize the git worktree %s folder\n", repoPath)
		errorMessage(err)
		return
	}
	repo := gitContainer{
		Tree:       worktree,
		Repository: r,
	}

	s3Client, err := getS3Client(*profileFlag, *regionFlag)
	if err != nil {
		return
	}

	rawObjects, err := queryS3Bucket(*bucketFlag, s3Client)
	sortedVersions, err := queryS3Versions(rawObjects, *outputFlag, s3Client)
	if err != nil {
		log.Println("Error getting versions of S3 Objects")
		errorMessage(err)
	}
	replayS3Changes(sortedVersions, s3Client, repo)
}
