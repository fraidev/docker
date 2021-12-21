package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
)

func main() {
	fmt.Println("Started")
	err := createSnapshot(false)
	if err != nil {
		log.Fatalln(err)
	}
	err = createSnapshot(true)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Finished")
}

func createSnapshot(rolling bool) error {
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmdToExecute := "/usr/local/bin/tezos-node"

	args := []string{"snapshot", "export", "--data-dir", "/var/run/tezos/node/data"}

	if rolling {
		args = append(args, "--rolling")
	}

	cmd := exec.Command(cmdToExecute, args...)
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return err
	}
	fmt.Println("Result: " + out.String())

	return nil
}
