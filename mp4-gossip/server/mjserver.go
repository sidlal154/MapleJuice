package server

import (
	"context"
	"io/ioutil"
	pb "mp4-gossip/protos/grpcServer"
	"mp4-gossip/queue"
)

func (s *Server) NotifyMaple(ctx context.Context, in *pb.NotifyMapleRequest) (*pb.NotifyMapleResponse, error) {

	s.logger.Info("[MJ-SERVER]", "Received a Maple command, will start it!!")
	executableName := in.Executable
	numMaples := in.NumMaples
	sdfsSrcDir := in.SdfsSrcDir
	interFilePrefix := in.InterFilePrefix

	go func() {
		err := s.Node.RunMaple(s.Config, s.logger, executableName, int(numMaples), interFilePrefix, sdfsSrcDir)
		if err != nil {
			return
		}
	}()

	resp := &pb.NotifyMapleResponse{
		Status: "Ok",
	}
	return resp, nil

}

func (s Server) NotifyJuice(ctx context.Context, in *pb.NotifyJuiceRequest) (*pb.NotifyJuiceResponse, error) {

	s.logger.Info("[MJ-SERVER]", "Received a Juice command, will start it!!")
	executableName := in.Executable
	numJuices := in.NumJuices
	sdfsDestFilename := in.SdfsDestFilename
	interFilePrefix := in.InterFilePrefix
	delFile := in.DeleteFile

	go func() {
		err := s.Node.RunJuice(s.Config, s.logger, executableName, int(numJuices), interFilePrefix, sdfsDestFilename, delFile)
		if err != nil {
			return
		}
	}()

	resp := &pb.NotifyJuiceResponse{
		Status: "Ok",
	}
	return resp, nil

}

func (s Server) AllotMaple(req *pb.AllotMapleRequest, stream pb.GrpcServer_AllotMapleServer) error {

	s.logger.Info("[MJ-SREVER]", "Got some Maple tasks from leader, need to do it")

	executableName := req.Executable
	sdfsSrcDir := req.SdfsSrcDir
	interFilePrefix := req.InterFilePrefix
	FromTo := req.FromTo

	for _, fromToEach := range FromTo {
		status := s.Node.RunMapleTask(s.Config, s.logger, executableName, fromToEach, sdfsSrcDir, interFilePrefix)
		response := &pb.AllotMapleResponse{Status: status}
		err := stream.Send(response)
		if err != nil {
			s.logger.Error("[MJ-SERVER]", "Some error in sending Maple ind-task response", err)
			return err
		}
		s.logger.Info("[MJ-SERVER]", "Completed Maple task for lines: "+fromToEach)
	}
	return nil
}

func (s Server) AllotJuice(req *pb.AllotJuiceRequest, stream pb.GrpcServer_AllotJuiceServer) error {
	s.logger.Info("[MJ-SREVER]", "Got some Juice tasks from leader, need to do it")

	executableName := req.Executable
	sdfsSrcDir := req.SdfsSrcDir

	for _, eachFile := range sdfsSrcDir {
		status := s.Node.RunJuiceTask(s.Config, s.logger, executableName, eachFile)
		response := &pb.AllotJuiceResponse{Status: status}
		err := stream.Send(response)
		if err != nil {
			s.logger.Error("[MJ-SERVER]", "Some error in sending Juice ind-task response", err)
			return err
		}
		s.logger.Info("[MJ-SERVER]", "Completed Juice task for keyFile: "+eachFile)
	}
	return nil
}

func (s Server) SendFile(stream pb.GrpcServer_SendFileServer) error {
	var byteStringArray []byte
	var filename string

	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}
		// Concatenate the received byte strings
		byteStringArray = append(byteStringArray, req.Data...)
		filename = req.Filename
	}
	s.logger.Info("[MJ-SERVER]", "Received a File from a worker: "+filename)

	err := ioutil.WriteFile(filename, byteStringArray, 0644)
	if err != nil {
		s.logger.Error("[SERVER]", "Some error while writing file in grpc", err)
		return err
	}

	response := &pb.SendWriteResponse{
		Status: "ok",
	}
	if err := stream.SendAndClose(response); err != nil {
		return err
	}

	return nil

}

func (s Server) NotifySQL(ctx context.Context, in *pb.NotifySQLRequest) (*pb.NotifySQLResponse, error) {

	for i, mapleReqs := range in.MapleRequests {
		if i == 0 {
			go func() {
				err := s.Node.RunMaple(s.Config, s.logger, mapleReqs.Executable, int(mapleReqs.NumMaples), mapleReqs.InterFilePrefix, mapleReqs.SdfsSrcDir)
				if err != nil {
					return
				}
			}()
		} else {
			queryEntry := queue.MJQueueEntry{
				Operation:   "MAPLE",
				Executable:  mapleReqs.Executable,
				WorkerCount: int(mapleReqs.NumMaples),
				InputFile:   mapleReqs.SdfsSrcDir,
				Prefix:      mapleReqs.InterFilePrefix,
				Delete:      false,
			}
			s.Node.SQLQueue.Enqueue(queryEntry)
		}
	}

	for _, juiceReqs := range in.JuiceRequests {
		queryEntry := queue.MJQueueEntry{
			Operation:   "JUICE",
			Executable:  juiceReqs.Executable,
			WorkerCount: int(juiceReqs.NumJuices),
			InputFile:   juiceReqs.SdfsDestFilename,
			Prefix:      juiceReqs.InterFilePrefix,
			Delete:      juiceReqs.DeleteFile,
		}
		s.Node.SQLQueue.Enqueue(queryEntry)
	}

	resp := &pb.NotifySQLResponse{
		Status: "Ok",
	}
	return resp, nil
}
