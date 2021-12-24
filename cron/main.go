package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func main() {
	// viper := viper.New()
	// viper.AddConfigPath(".")
	// viper.SetConfigName("config")
	// viper.SetConfigType("env")
	// err := viper.ReadInConfig()

	bucketName := "tezos-snapshot-bucket"
	// maxDays := 3

	// bucketName := viper.GetString("BUCKET_NAME")
	// maxDays := viper.GetInt("MAX_DAYS")
	ctx := context.Background()

	// fmt.Println("Creating full snapshot now")

	// // Create Snapshots
	// err := createSnapshot(false)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	// fmt.Println("Creating rolling snapshot now")
	// err := createSnapshot(true)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	os.Create("asdsadsa.full")
	os.Create("dsadsadsad.rolling")

	fmt.Println("get snapshot names")
	snapshotfileNameFull, snapshotfileNamesRolling, err := getSnapshotNames()
	if err != nil {
		log.Fatalln(err.Error())
	}

	fmt.Printf("snapshotfileNameFull: %v \n", snapshotfileNameFull)
	fmt.Printf("snapshotfileNamesRolling: %v \n", snapshotfileNamesRolling)

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create folder

	// fmt.Println("Getting Files")
	// // Open local fileFull.
	// fileFull, err := os.Open(snapshotfileNameFull)
	// if err != nil {
	// 	log.Fatalf("os.Open: %v", err)
	// }
	// defer fileFull.Close()

	// // Upload an snapshot
	// fmt.Println("Uploading snapshot")
	// err = uploadSnapshot(ctx, client, bucketName, fileFull)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	// Open local file.
	fmt.Println("Getting Files")
	fileRolling, err := os.Open(snapshotfileNamesRolling)
	if err != nil {
		log.Fatalf("os.Open: %v", err)
	}
	defer fileRolling.Close()

	// Upload an snapshot
	fmt.Println("Uploading snapshot")
	err = uploadSnapshot(ctx, client, bucketName, fileRolling)
	if err != nil {
		log.Fatalf("Failed to upload snapshot: %v", err)
	}

	// Delete local files
	// fmt.Println("Deleting snapshot file full")
	// err = os.Remove(snapshotfileNameFull)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println("Deleting snapshot file rolling")
	err = os.Remove(snapshotfileNamesRolling)
	if err != nil {
		log.Fatal(err)
	}

	// Delete cloud old Files
	// fmt.Println("Deleting old snapshots")
	// deleteOldSnapshots(ctx, client, bucketName, maxDays)
}

func createSnapshot(rolling bool) error {
	bin := "/usr/local/bin/tezos-node"

	args := []string{"snapshot", "export", "--data-dir", "/var/run/tezos/node/data"}

	if rolling {
		args = append(args, "--rolling")
	}

	var errBuf, outBuf bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stderr = io.MultiWriter(os.Stderr, &errBuf)
	cmd.Stdout = io.MultiWriter(os.Stdout, &outBuf)
	err := cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func getSnapshotNames() (string, string, error) {
	var errBuf, outBuf bytes.Buffer
	cmd := exec.Command("/bin/ls", "-1a")
	cmd.Stderr = io.MultiWriter(os.Stderr, &errBuf)
	cmd.Stdout = io.MultiWriter(os.Stdout, &outBuf)
	err := cmd.Run()
	if err != nil {
		return "", "", err
	}
	snapshotfileNames := strings.Split(outBuf.String(), "\n")

	fmt.Print(outBuf.String())
	fmt.Printf("len: %d \n", len(snapshotfileNames))

	var rolling, full string

	for _, fileName := range snapshotfileNames {
		if strings.Contains(fileName, "rolling") {
			rolling = fileName
		}
		if strings.Contains(fileName, "full") {
			full = fileName
		}
	}

	return full, rolling, nil
}

func uploadSnapshot(ctx context.Context, client *storage.Client, bucketName string, file *os.File) error {
	currentTime := time.Now()
	currentDate := currentTime.Format("2006.01.02")

	fmt.Printf("Current Date is %q.\n", currentDate)

	objectHandler := client.Bucket(bucketName).Object(currentDate + "/" + file.Name())
	writer := objectHandler.NewWriter(ctx)
	if _, err := io.Copy(writer, file); err != nil {
		fmt.Printf("Error Write Copy")
		return err
	}
	if err := writer.Close(); err != nil {
		fmt.Printf("Error Write Close")
		return err
	}
	fmt.Printf("Blob %q uploaded.\n", file.Name())

	// Make this file public
	acl := objectHandler.ACL()
	if err := acl.Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return err
	}
	fmt.Printf("Blob %q is public now.\n", file.Name())

	return nil
}

func deleteOldSnapshots(ctx context.Context, client *storage.Client, bucketName string, maxDays int) error {
	it := client.Bucket(bucketName).Objects(ctx, &storage.Query{})

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("listBucket: unable to list bucket %q: %v", bucketName, err)
			return err
		}
		deleteFile(ctx, client, bucketName, maxDays, obj)
	}

	return nil
}

func deleteFile(ctx context.Context, client *storage.Client, bucketName string, maxDays int, obj *storage.ObjectAttrs) error {
	paths := strings.Split(obj.Name, "/")

	if len(paths) <= 0 {
		return fmt.Errorf("invalid file name %q", obj.Name)
	}

	folderName := paths[0]
	t, err := time.Parse("2006.01.02", folderName)
	if err != nil {
		return err
	}

	diff := time.Now().Sub(t)
	diffDays := int(diff.Hours() / 24)
	fmt.Printf("%d \n", diffDays)

	if maxDays >= diffDays {
		objHandler := client.Bucket(bucketName).Object(obj.Name)
		err = objHandler.Delete(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Object(%q).Delete: %v", obj.Name, err)
	}
	return nil
}
