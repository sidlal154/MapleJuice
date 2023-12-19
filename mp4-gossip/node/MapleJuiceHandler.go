package node

import (
	"fmt"
	"io/ioutil"
	"mp4-gossip/config"
	"mp4-gossip/logger"
	pb "mp4-gossip/protos/grpcServer"
	"mp4-gossip/utils"
	"path/filepath"
	"strconv"
	"strings"
	"os"
	"time"
)

func (n *Node) MapleCheckerThread(config *config.Config, logger *logger.CustomLogger, executableName string, interFilePrefix string) {
	time.Sleep(5 * time.Second)
	for {
		mapleTracker := n.GetMapleTrackerCopy()
		status := 0
		for _, value := range mapleTracker {
			if n.failStop {
				time.Sleep(5 * time.Second)
				n.failStop = false
			}
			//if the node is in the membership list, only then check (TODO: No, handle this in Gossip)
			for _, task := range value {
				if task.Status == "ongoing" && task.TaskName == executableName {
					//logger.Info("[MJ-NODE]","This task is still going, so cant combine files Right now: "+task.TaskRange)
					status = 1
					break
				}
			}
		}

		if status == 0 {
			logger.Info("[NODE-M-CHECKER", "Maple Task Complete: "+executableName)
			break
		}
	}

	mapleTracker := n.GetMapleTracker()
	for key:= range mapleTracker{
		delete(mapleTracker, key)
	}

	n.PutMapleTracker(mapleTracker)
	//TODO: file combining and sdfs uploading step to be writted by rrs7
	MappleFilesCombine(interFilePrefix, config, logger)
	files, err := ioutil.ReadDir(config.LocalDir)
	if err != nil {
		logger.Error("[MapleCheckerThread]", "Unable to read files from Local:", err)
	}
	for _, file := range files {
		fileName := file.Name()
		parts := strings.Split(strings.TrimSuffix(fileName, ".csv"), "_")
		if len(parts) == 2 && parts[0] == interFilePrefix {
			n.Upload(fileName, fileName, config, logger)
		}
		if len(parts) == 4 {
			fmt.Println("===============================Deleting:", config.LocalDir+"/"+fileName)
			os.Remove(config.LocalDir + "/" + fileName)
		}
	}

	if !n.SQLQueue.IsEmpty() {
		qEntry := n.SQLQueue.Dequeue()

		if qEntry.Operation == "MAPLE" {
			go func() {
				err := n.RunMaple(config, logger, qEntry.Executable, qEntry.WorkerCount, qEntry.Prefix, qEntry.InputFile)
				if err != nil {
					return
				}
			}()
		} else if qEntry.Operation == "JUICE" {
			go func() {
				err := n.RunJuice(config, logger, qEntry.Executable, qEntry.WorkerCount, qEntry.Prefix, qEntry.InputFile, qEntry.Delete)
				if err != nil {
					return
				}
			}()
		}
	}
}

// RunMaple This will be called by Gprc via any node
// In principle, this code is run only on the leader
func (n *Node) RunMaple(config *config.Config, logger *logger.CustomLogger, executableName string, numMaples int, interFilePrefix string, sdfsSrcDir string) error {

	//Launch thread which tracks if this maple task is complete, and combine them once it is
	go n.MapleCheckerThread(config, logger, executableName, interFilePrefix)

	//I have received a request from somebody to run these numMaple tasks
	files, err := ioutil.ReadDir(config.LocalDir)
	if err != nil {
		logger.Error("[MJ-NODE]", "Invalid SDFS directory: ", err)
		return err
	}

	var inputFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), sdfsSrcDir) {
			inputFiles = append(inputFiles, file.Name())
		}
	}

	inputFile := inputFiles[0]
	content, err := ioutil.ReadFile(config.LocalDir + "/" + inputFile)
	if err != nil {
		logger.Error("[MJ-NODE]", "Invalid SDFS file: ", err)
		return err
	}
	lines := strings.Split(string(content), "\n")
	lineCount := len(lines) - 1

	//taskDiv := lineCount / numMaples
	taskDiv := utils.CeilDivide(lineCount, numMaples)
	memList := n.GetMembershipList()
	members := memList.Member
	memIndex := 0
	//Round robin allocation
	for i := 1; i <= numMaples; i++ {
		mapleTracker := n.GetMapleTracker()

		if memIndex == len(members) {
			memIndex = 0
		}
		if members[memIndex].NodeId == n.NodeId{
                        memIndex++
                }
		member := members[memIndex]
		memIndex++
		linesFrom := (i-1)*taskDiv + 1
		linesTo := i * taskDiv

		trackerEntry := &utils.TaskTracker{
			TaskName:  executableName,
			TaskRange: strconv.Itoa(linesFrom) + "," + strconv.Itoa(linesTo),
			InputFile: inputFile,
			Prefix:    interFilePrefix,
			Status:    "ongoing",
		}

		//Comment this faltu line later
		fmt.Println("Task "+executableName+" assigned to "+member.NodeId+" For task range: ", linesFrom)

		mapleTracker[member.NodeId] = append(mapleTracker[member.NodeId], trackerEntry)
		n.PutMapleTracker(mapleTracker)

	}

	memberAlloted := n.GetMapleTrackerKey()

	for _,memberId := range memberAlloted {
		//trackerTasks := trackerTasks
		memberId := memberId
		go func() {
			in := n.GetMapleTracker()
			trackerTasks := in[memberId]
			var rangeTasks []string
			for _, task := range trackerTasks {
				rangeTask := task.TaskRange
				rangeTasks = append(rangeTasks, rangeTask)
			}

			req := &pb.AllotMapleRequest{
				Executable:      executableName,
				SdfsSrcDir:      inputFile,
				InterFilePrefix: interFilePrefix,
				FromTo:          rangeTasks,
			}
			workerAddress := strings.Split(memberId, "#")[0]

			_, retTrackerTasks, err := n.GrpcClient.ClientAllotMaple(trackerTasks, workerAddress, req)
			if err != nil {
				logger.Error("[MJ-NODE]", "unable to receive correct responses, please do rescehduling: ", err)
				return
			}

			in[memberId] = retTrackerTasks
			n.PutMapleTracker(in)
		}()

	}

	logger.Info("[MJ-NODE]", "All Maple requests have been allotted")

	return nil
}

func (n *Node) JuiceCheckerThread(config *config.Config, logger *logger.CustomLogger, executableName string, sdfsDestFilename string, delFile bool, interFilePrefix string) {
	logger.Info("[MJ-NODE]","INSIDE JUICECHECKERTHREAD: ===")
	for {
		juiceTracker := n.GetJuiceTrackerCopy()
		status := 0
		for _, value := range juiceTracker {
			//if the node is in the membership list, only then check (TODO: No, handle this in Gossip)
			for _, task := range value {
				//fmt.Println("********** executable:" ,executableName)
				//fmt.Println("********** taskName:" ,task.TaskName)
				if task.Status == "ongoing" && task.TaskName == executableName {
				//	logger.Info("[MJ-NODE]","This task is still going, so cant combine files Right now: "+task.TaskRange)
					status = 1
					break
				}
			}
		}

		if status == 0 {
			logger.Info("[NODE-J-CHECKER", "Juice Task Complete: "+executableName)
			break
		}
	}

	juiceTracker := n.GetJuiceTracker()
	for key := range juiceTracker{
		delete(juiceTracker, key)
	}

	n.PutJuiceTracker(juiceTracker)
	//TODO: file combining and sdfs uploading step to be writted by rrs7
	//sdfsDestFilename is where combined file needs to be written
	//delFile is if to delete the intermediary files
	JuiceFilesCombine(interFilePrefix, sdfsDestFilename, config, logger)
	files, err := ioutil.ReadDir(config.LocalDir)
	if err != nil {
		logger.Error("[JuiceCheckerThread]", "Unable to read files from Local:", err)
	}
	for _, file := range files {
		fileName := file.Name()
		//fmt.Println("FileName:", file.Name())
		//fmt.Println("REQUIRED:", sdfsDestFilename)
		
		if fileName == sdfsDestFilename+".csv" {
			n.Upload(fileName, fileName, config, logger)
		}
	}
	if delFile {
		for _, file := range files {
			fileName := file.Name()
			parts := strings.Split(fileName, "_")
			if len(parts) == 3 {
				if parts[0] == interFilePrefix {
					n.FileDelete(fileName, config, logger)
				}
			}
		}
	}

}

func (n *Node) RunJuice(config *config.Config, logger *logger.CustomLogger, executableName string, numJuices int, interFilePrefix string, sdfsDestFilename string, delFile bool) error {

	//Launch thread which tracks if this juice task is complete, and combine them once it is
	go n.JuiceCheckerThread(config, logger, executableName, sdfsDestFilename, delFile, interFilePrefix)

	files, err := ioutil.ReadDir(config.SdfsDir)
	if err != nil {
		logger.Error("[MJ-NODE]", "Invalid local directory: ", err)
		return err
	}

	var intFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), interFilePrefix) {
			intFiles = append(intFiles, file.Name())
		}
	}

	countKeys := len(intFiles)
	//var taskDiv int
	if countKeys <= numJuices {
		numJuices = countKeys
		//taskDiv = 1
	}

	memList := n.GetMembershipList()
	members := memList.Member
	memIndex := 0
	intFilesIndex := 0
	//round robin range based allocation
	for i := 1; i <= numJuices; i++ {
		if memIndex == len(members) {
			memIndex = 0
		}
		if members[memIndex].NodeId == n.NodeId{
                        memIndex++
                }
		member := members[memIndex]
		memIndex++

		keyFileAlloted := intFiles[intFilesIndex]
		intFilesIndex++

		trackerEntry := &utils.TaskTracker{
			TaskName:  executableName,
			TaskRange: keyFileAlloted,
			Status:    "ongoing",
		}

		//Comment this faltu line later
		fmt.Println("Task "+executableName+" assigned to "+member.NodeId+" For task keyFile: ", keyFileAlloted)
		juiceTracker := n.GetJuiceTracker()
		juiceTracker[member.NodeId] = append(juiceTracker[member.NodeId], trackerEntry)
		n.PutJuiceTracker(juiceTracker)

	}

	juiceTracker := n.GetJuiceTrackerKey()
	for _, memberId:= range juiceTracker {
		//trackerTasks := trackerTasks
		memberId := memberId
		go func() {
			var rangeTasks []string
			in := n.GetJuiceTracker()
			trackerTasks := in[memberId]
			for _, task := range trackerTasks {
				rangeTask := task.TaskRange
				rangeTasks = append(rangeTasks, rangeTask)
			}

			req := &pb.AllotJuiceRequest{
				Executable: executableName,
				SdfsSrcDir: rangeTasks,
			}
			workerAddress := strings.Split(memberId, "#")[0]

			_, retTrackerTasks, err := n.GrpcClient.ClientAllotJuice(trackerTasks, workerAddress, req)
			if err != nil {
				logger.Error("[MJ-NODE]", "unable to receive correct responses, please do rescehduling: ", err)
				return
			}

			in[memberId] = retTrackerTasks
			n.PutJuiceTracker(in)
		}()

	}

	logger.Info("[MJ-NODE]", "All Juice requests have been allotted")

	return nil
}

func (n *Node) RunMapleTask(config *config.Config, logger *logger.CustomLogger, executableName string, FromTo string, sdfsSrcDir string, interFilePrefix string) string {
	//TODO: rrs7: do maple os.exec here, run the executables, and send the file via n.GrpcClient.ClientSendFile
	arguments := strings.Split(executableName, " ")
	executable := arguments[0]
	targetColumn := arguments[1]
	regexp := arguments[2]
	prefix := arguments[3]
	worker := arguments[4]
	srcFolder := arguments[5]
	finalColumn := arguments[6]

	//sdfsSrcDirectory == file name?
	file := sdfsSrcDir

	numbers := strings.Split(FromTo, ",")
	task := FromTo

	//Invoke EXECUTABLE
	//Download(to_be_local_name, sdfs_name, config, logger)
	n.Download(file, file, config, logger)
	n.Download(executable, executable, config, logger)
	n.MappleInvoker(executable, targetColumn, regexp, prefix, worker, srcFolder, finalColumn, file, numbers[0], numbers[1], task, logger)
	logger.Info("[Executable-Done]", "MappleTask Done")

	filePrefixScan := prefix + "_" + worker + "_" + task + "_"
	files, err := ListFilesWithPrefixAndExtension(config.LocalDir, filePrefixScan, ".csv")
	if err != nil {
		logger.Error("[RunMapleTask]", "Error with file extension match: ", err)
	}
	for _, file := range files {
		byteStringArray, err := ioutil.ReadFile(file)
		if err != nil {
			logger.Error("[RunMapleTask - sending file]", "Error with reading file: ", err)
			continue
		}
		req := &pb.SendWriteRequest{
			Filename: file,
			Data:     byteStringArray,
		}
		resp, err := n.GrpcClient.ClientSendFile(config.Introducer, req)
		if err!=nil{
			fmt.Println("ERROR while sending Maple file")
		}
		fmt.Println("Response STATUS-MAPPLE SEND", resp.Status)
	}
	return FromTo
}

// return filename
func (n *Node) RunJuiceTask(config *config.Config, logger *logger.CustomLogger, executableName string, fileKey string) string {
	//TODO: rrs7: do juice os.exec here, run the executables, and send the file via n.GrpcClient.ClientSendFile
	arguments := strings.Split(executableName, " ")
	executable := arguments[0]
	finalFile := arguments[1]
	fmt.Println("***************************")
	fmt.Println("FileKey: ", fileKey)
	fmt.Println("Executable:", executable)
	fmt.Println("finalFile:", finalFile)
	fmt.Println("***************************")
	// srcFolder := arguments[2]
	//Download(to_be_local_name, sdfs_name, config, logger)
	n.Download(fileKey, fileKey, config, logger)
	n.Download(executable, executable, config, logger)
	n.JuiceInvoker(executable, fileKey, finalFile, config, logger)

	filePrefixScan := finalFile
	files, err := ListFilesWithPrefixAndExtension(config.LocalDir, filePrefixScan, ".csv")
	if err != nil {
		logger.Error("[RunJuiceTask]", "Error with file extension match: ", err)
	}
	for _, file := range files {
		byteStringArray, err := ioutil.ReadFile(file)
		if err != nil {
			logger.Error("[RunJuiceTask - sending file]", "Error with reading file: ", err)
			continue
		}
		req := &pb.SendWriteRequest{
			Filename: file,
			Data:     byteStringArray,
		}
		n.GrpcClient.ClientSendFile(config.Introducer, req)
	}
	return fileKey
}

func ListFilesWithPrefixAndExtension(directory, prefix, extension string) ([]string, error) {
	var fileList []string

	files, err := filepath.Glob(filepath.Join(directory, prefix+"*"+extension))
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileList = append(fileList, file)
	}

	return fileList, nil
}

func MappleFilesCombine(interFilePrefix string, config *config.Config, logger *logger.CustomLogger) {
	// Specify your directory path
	dirPath := config.LocalDir

	// List all files in the directory
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}
	logger.Info("[FILES-Combine]", "File list fetched")
	// Create a map to store file content by group
	fileContentMap := make(map[string]string)

	// Iterate through files
	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".csv") {
			// Extract parts from the file name
			parts := strings.Split(strings.TrimSuffix(fileName, ".csv"), "_")
			if len(parts) >= 4 && parts[0] == interFilePrefix {
				// Create a group key using the first and last parts of the file name
				groupKey := fmt.Sprintf("%s_%s", parts[0], parts[len(parts)-1])

				// Read file content
				content, err := ioutil.ReadFile(filepath.Join(dirPath, fileName))
				if err != nil {
					fmt.Println("Error reading file:", err)
					continue
				}

				// Combine content for files with the same group key
				fileContentMap[groupKey] += string(content)
			}
		}
	}

	logger.Info("[FILES-Combine]", "Files combined")
	// You can do something with the combined content, e.g., write to a new file
	for groupKey, content := range fileContentMap {
		outputFileName := fmt.Sprintf("%s.csv", groupKey)
		outputFilePath := filepath.Join(dirPath, outputFileName)

		err := ioutil.WriteFile(outputFilePath, []byte(content), 0644)
		if err != nil {
			fmt.Println("Error writing combined file:", err)
			continue
		}

		fmt.Printf("Combined file %s created.\n", outputFileName)
	}
}

func JuiceFilesCombine(interFilePrefix string, destName string, config *config.Config, logger *logger.CustomLogger) {
	// Specify your directory path
	dirPath := config.LocalDir

	// List all files in the directory
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		fmt.Println("JUICE - Error reading directory:", err)
		return
	}
	logger.Info("[FILES-Combine-JUICE]", "File list fetched")
	// Create a map to store file content by group
	fileContentMap := make(map[string]string)

	// Iterate through files
	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".csv") {
			// Extract parts from the file name
			parts := strings.Split(strings.TrimSuffix(fileName, ".csv"), "_")
			if len(parts) == 3 && parts[0] == interFilePrefix && parts[1] == "R" {
				// Create a group key using the first and last parts of the file name
				groupKey := fmt.Sprintf("%s", parts[0])

				// Read file content
				content, err := ioutil.ReadFile(filepath.Join(dirPath, fileName))
				if err != nil {
					fmt.Println("Error reading file:", err)
					continue
				}

				// Combine content for files with the same group key
				fileContentMap[groupKey] += string(content)
			}
		}
	}

	logger.Info("[FILES-Combine]", "Files combined")
	// You can do something with the combined content, e.g., write to a new file
	for _, content := range fileContentMap {
		outputFileName := fmt.Sprintf("%s", destName)
		outputFilePath := filepath.Join(dirPath, outputFileName)

		err := ioutil.WriteFile(outputFilePath, []byte(content), 0644)
		if err != nil {
			fmt.Println("Error writing combined file:", err)
			continue
		}

		fmt.Printf("Combined file %s created.\n", outputFileName)
	}
}

func (n *Node) RescheduleMapleTask(config *config.Config, logger *logger.CustomLogger, memberId string, task *utils.TaskTracker) {
	//Comment this faltu line later
	mapleTracker := n.GetMapleTracker()
	fmt.Println("Task "+task.TaskName+" re-assigned to "+memberId+" For task range: ", task.TaskRange)

	mapleTracker[memberId] = append(mapleTracker[memberId], task)
	n.PutMapleTracker(mapleTracker)
	go func() {
		in := n.GetMapleTracker()
		trackerTasks := in[memberId]
		var rangeTasks []string
		for _, task := range trackerTasks {
			rangeTask := task.TaskRange
			rangeTasks = append(rangeTasks, rangeTask)
		}

		req := &pb.AllotMapleRequest{
			Executable:      task.TaskName,
			SdfsSrcDir:      task.InputFile,
			InterFilePrefix: task.Prefix,
			FromTo:          rangeTasks,
		}
		workerAddress := strings.Split(memberId, "#")[0]

		_, retTrackerTasks, err := n.GrpcClient.ClientAllotMaple(trackerTasks, workerAddress, req)
		if err != nil {
			logger.Error("[MJ-NODE]", "unable to receive correct responses, please do rescehduling: ", err)
			return
		}

		in[memberId] = retTrackerTasks
		n.PutMapleTracker(in)
	}()
}

func (n *Node) RescheduleJuiceTask(config *config.Config, logger *logger.CustomLogger, memberId string, task *utils.TaskTracker) {
	juiceTracker := n.GetJuiceTracker()
	fmt.Println("Task "+task.TaskName+" re-assigned to "+memberId+" For task range: ", task.TaskRange)
	juiceTracker[memberId] = append(juiceTracker[memberId], task)
	n.PutJuiceTracker(juiceTracker)

	go func() {
		var rangeTasks []string
		in := n.GetJuiceTracker()
		trackerTasks := in[memberId]
		for _, task := range trackerTasks {
			rangeTask := task.TaskRange
			rangeTasks = append(rangeTasks, rangeTask)
		}

		req := &pb.AllotJuiceRequest{
			Executable: task.TaskName,
			SdfsSrcDir: rangeTasks,
		}
		workerAddress := strings.Split(memberId, "#")[0]

		_, retTrackerTasks, err := n.GrpcClient.ClientAllotJuice(trackerTasks, workerAddress, req)
		if err != nil {
			logger.Error("[MJ-NODE]", "unable to receive correct responses, please do rescehduling: ", err)
			return
		}

		in[memberId] = retTrackerTasks
		n.PutJuiceTracker(in)
	}()
}
