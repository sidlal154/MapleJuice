package node

import (
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	pb "mp4-gossip/protos/grpcServer"
	"regexp"
)

// MappleInvoker("filterMapple2", words[3], 5, 10, words[5], words[7], prefix, 1, 1, "nil", config, logger, filelogger)
func (n *Node) MappleCaller(executable string, numMaple int, prefix string, folder string, namedKeyMap map[string]string, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {

	toGrpc := executable + " " + namedKeyMap["targetColumn"] + " " + namedKeyMap["regExp"] + " " + prefix + " " + extractNumbers(config.MachineName) + " " + config.LocalDir + " " + namedKeyMap["col"]
	req := &pb.NotifyMapleRequest{
		Executable:      toGrpc,
		NumMaples:       int64(numMaple),
		InterFilePrefix: prefix,
		SdfsSrcDir:      folder,
	}
	resp, err := n.GrpcClient.ClientNotifyMaple(config.Introducer, req)
	if err != nil {
		logger.Error("[MJ-MappleCaller]", "Unable to invoke GRPC: ", err)
	}
	logger.Info("[MJ-MappleCaller]", resp.Status)
	//what do we do with response?
}

func (n *Node) JuiceCaller(executable string, numJuice int, prefix string, finalFile string, delete string, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	toGrpc := executable + " " + finalFile + " " + config.LocalDir
	toDelete := false
	if delete == "1" {
		toDelete = true
	}
	req := &pb.NotifyJuiceRequest{
		Executable:       toGrpc,
		NumJuices:        int64(numJuice),
		InterFilePrefix:  prefix,
		SdfsDestFilename: finalFile,
		DeleteFile:       toDelete,
	}
	resp, err := n.GrpcClient.ClientNotifyJuice(config.Introducer, req)
	if err != nil {
		logger.Error("[MJ-JuiceCaller]", "Unable to invoke GRPC: ", err)
	}
	logger.Info("[MJ-JuiceCaller]", resp.Status)
	//what to do with response?
}

func (n *Node) SQLCaller(executable string, numMaple int, numJuice int, prefix string, folder string, namedKeyMap map[string]string, delete int, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	if executable == "FILTER" {
		toDelete := false
		if delete == 1 {
			toDelete = true
		}
		toGrpc := "filterMapple2" + " " + namedKeyMap["targetColumn"] + " " + namedKeyMap["regExp"] + " " + prefix + " " + extractNumbers(config.MachineName) + " " + config.LocalDir + " " + "-1"
		toGrpc2 := "filterJuice2" + " " + namedKeyMap["final"] + " " + config.LocalDir
		req1 := &pb.NotifyMapleRequest{
			Executable:      toGrpc,
			NumMaples:       int64(numMaple),
			InterFilePrefix: prefix,
			SdfsSrcDir:      folder,
		}
		req2 := &pb.NotifyJuiceRequest{
			Executable:       toGrpc2,
			NumJuices:        int64(numJuice),
			InterFilePrefix:  prefix,
			SdfsDestFilename: namedKeyMap["final"],
			DeleteFile:       toDelete,
		}
		finalreq := &pb.NotifySQLRequest{
			MapleRequests: []*pb.NotifyMapleRequest{req1},
			JuiceRequests: []*pb.NotifyJuiceRequest{req2},
		}
		resp, err := n.GrpcClient.ClientNotifySQL(config.Introducer, finalreq)
		if err != nil {
			logger.Error("[MJ-SQLCaller]", "Some error: ", err)
		}
		logger.Info("[MJ-SQLCaller]", resp.Status)
		//what do we do with response?
	} else if executable == "JOIN" {
		toDelete := false
		if delete == 1 {
			toDelete = true
		}
		toGrpc := "joinMapple" + " " + namedKeyMap["targetColumn"] + " -1 " + prefix + " " + extractNumbers(config.MachineName) + " " + config.LocalDir + " " + "1"
		toGrpc2 := "joinMapple" + " " + namedKeyMap["targetColumn2"] + " -1 " + prefix + " " + extractNumbers(config.MachineName) + " " + config.LocalDir + " " + "2"
		toGrpc3 := "joinJuice" + " " + namedKeyMap["final"] + " " + config.LocalDir

		req1 := &pb.NotifyMapleRequest{
			Executable:      toGrpc,
			NumMaples:       int64(numMaple),
			InterFilePrefix: prefix,
			SdfsSrcDir:      folder,
		}

		req2 := &pb.NotifyMapleRequest{
			Executable:      toGrpc2,
			NumMaples:       int64(numMaple),
			InterFilePrefix: prefix,
			SdfsSrcDir:      namedKeyMap["Data2"],
		}
		req3 := &pb.NotifyJuiceRequest{
			Executable:       toGrpc3,
			NumJuices:        int64(numJuice),
			InterFilePrefix:  prefix,
			SdfsDestFilename: namedKeyMap["final"],
			DeleteFile:       toDelete,
		}

		finalreq := &pb.NotifySQLRequest{
			MapleRequests: []*pb.NotifyMapleRequest{req1, req2},
			JuiceRequests: []*pb.NotifyJuiceRequest{req3},
		}
		resp, err := n.GrpcClient.ClientNotifySQL(config.Introducer, finalreq)
		if err != nil {
			logger.Error("[MJ-SQLCaller]", "Some error: ", err)
		}
		logger.Info("[MJ-SQLCaller]", resp.Status)
		//what do we do with response?
	}

}

func extractNumbers(input string) string {
	re := regexp.MustCompile(`-(\d+)`)
	matches := re.FindAllStringSubmatch(input, -1)

	var numbers []string
	for _, match := range matches {
		numbers = append(numbers, match[1])
	}

	return numbers[0]
}
