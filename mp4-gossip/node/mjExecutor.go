package node

import (
	"fmt"
	"mp4-gossip/config"
	"mp4-gossip/logger"
	"os"
	"os/exec"
	"path/filepath"
)

// for test1 finalColumn - Column on which grouping is done
// for test2 finalColumn - -1
// for join finalColumn - DatsetNumber

// must be node function
func (n *Node) MappleInvoker(executable string, targetColumn string, regexp string, prefix string, worker string, srcFolder string, finalColumn string, file string, startLine string, endLine string, task string, logger *logger.CustomLogger) {
	executableFolder := srcFolder
	// Specify the name of the Go program you want to run
	dir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current working directory:", err)
		return
	}
	fmt.Println(dir)
	executableName := executable
	// start := strconv.Itoa(startLine)
	// end := strconv.Itoa(endLine)
	// workerNo := strconv.Itoa(worker)
	// taskNo := strconv.Itoa(task)
	// regexp = regexp
	executableFolder = filepath.Join(dir, executableFolder)
	executablePath := filepath.Join(executableFolder, executableName)
	inputFile := filepath.Join(executableFolder, file)
	// programPath := filepath.Join(dir, programName)

	// Check if the program file exists

	// fmt.Println(executablePath, inputFile, start, end, targetColumn, workerNo, taskNo, regexp)

	if _, err := os.Stat(executablePath); os.IsNotExist(err) {
		fmt.Println("Error: The specified Go program does not exist.")
		return
	}

	// Run the Go program
	cmd := exec.Command(executablePath, targetColumn, regexp, prefix, worker, executableFolder, finalColumn, inputFile, startLine, endLine, task)
	cmd.Dir = executableFolder
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		fmt.Println("Error running the Go program:", err)
		return
	}
	fmt.Println("Mapple task SUCCESS")
}

func (n *Node) JuiceInvoker(executable string, file string, prefix string, config *config.Config, logger *logger.CustomLogger) {
	executableFolder := config.LocalDir
	// Specify the name of the Go program you want to run
	dir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current working directory:", err)
		return
	}
	// fmt.Println(dir)
	executableName := executable
	executableFolder = filepath.Join(dir, executableFolder)
	executablePath := filepath.Join(executableFolder, executableName)
	inputFile := filepath.Join(executableFolder, file)
	// programPath := filepath.Join(dir, programName)

	// Check if the program file exists

	// fmt.Println(executablePath, inputFile, start, end, targetColumn, workerNo, taskNo, regexp)

	if _, err := os.Stat(executablePath); os.IsNotExist(err) {
		fmt.Println("Error: The specified Go program does not exist.")
		return
	}

	// Run the Go program
	cmd := exec.Command(executablePath, inputFile, prefix, executableFolder)
	cmd.Dir = executableFolder
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		fmt.Println("Error running the Go program:", err)
		return
	}
}
