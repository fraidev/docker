package main

import (
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

func createSnapshot(roling bool) error {
	cmdToExecute := "/usr/local/bin/tezos-node"

	args := []string{"snapshot", "export", "--data-dir", "/var/run/tezos/node/data"}

	if roling {
		args = append(args, "--roling")
	}

	cmd := exec.Command(cmdToExecute, args...)
	stdout, err := cmd.Output()
	if err != nil {
		return err
	}
	fmt.Println(string(stdout))

	return nil
}
