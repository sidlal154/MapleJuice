package server

import (
	"context"
	"google.golang.org/grpc"
	"io/ioutil"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/node"
	pb "mp4-gossip/protos/grpcServer"
	"mp4-gossip/utils"
	"net"
	"os"
	"strings"
	"sync"
)

type Server struct {
	pb.UnimplementedGrpcServerServer
	logger *logger.CustomLogger
	Node   *node.Node
	Config *config.Config
}

func NewServer(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger, mode int) *Server {
	Node := node.StartNewNode(config, logger, filelogger, mode)
	return &Server{
		logger: logger,
		Node:   Node,
		Config: config,
	}
}

func (s *Server) LaunchNode(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	var wg sync.WaitGroup

	//filelogger.Info("[MAIN]", "Membership List first element: "+s.Node.GetMembershipList().Member[0].NodeId)
	if config.MachineName != config.Introducer {
		wg.Add(1)
		go s.Node.Introduce(config, logger, filelogger, &wg)
		wg.Wait()
	}

	wg.Add(2)
	go s.Node.ClientImpl(&wg, logger, config, filelogger)

	// Start of mp4
	go s.Node.ServerImpl(&wg, logger, config, filelogger)
	logger.Info("[MAIN]", "Started Gossip Protocol")
}

func (s *Server) LaunchServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		s.logger.Error("[SERVER]", "Failed to listen:", err)
		return
	}
	serv := grpc.NewServer(grpc.MaxRecvMsgSize(3 * 1024 * 1024 * 1024))
	pb.RegisterGrpcServerServer(serv, s)
	s.logger.Info("[SERVER]", "Grpc Server Listening on :50051")
	if err := serv.Serve(lis); err != nil {
		s.logger.Error("[SERVER]", "Failed to serve:", err)
		return
	}
}

func (s *Server) SendRead(ctx context.Context, in *pb.SendReadRequest) (*pb.SendReadResponse, error) {

	filename := in.Filename
	s.logger.Info("[SERVER]", "Received a read request, sending the correct nodeId for file:"+filename)

	//a function call to get the machine IP where the file is present
	memList := s.Node.GetMembershipList()
	var peers []string
	var leader string

	//Find leader
	for _, member := range memList.Member {
		if utils.CheckInArray(filename, member.LeaderList) {
			leader = member.NodeId
			peers = append(peers, strings.Split(member.NodeId, "#")[0])
			break
		}
	}

	//Add slaves
	for _, member := range memList.Member {
		if utils.CheckInArray(filename, member.FileList) && member.NodeId != leader {
			peers = append(peers, strings.Split(member.NodeId, "#")[0])
		}
	}

	peerId := strings.Join(peers, ",")
	s.logger.Info("[SERVER]", "peers on which you can check(fist peer is the leader): "+peerId)
	resp := &pb.SendReadResponse{
		DestinationId: peerId,
	}
	return resp, nil
}

func (s *Server) GetReadFile(req *pb.GetReadFileRequest, stream pb.GrpcServer_GetReadFileServer) error {
	filename := req.Filename
	s.logger.Info("[SERVER]", "Received a readFile request, sending the file for fileId:"+filename)

	for {

		// Generate and send a byte array response to the client

		dataToSend, err := ioutil.ReadFile(s.Config.SdfsDir + "/" + filename)
		if err != nil {
			s.logger.Error("[SERVER]", "Error while reading the file for fileId:"+filename, err)
			return err
		}
		response := &pb.GetByteArrayResponse{Data: dataToSend}
		if err := stream.Send(response); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) SendWrite(stream pb.GrpcServer_SendWriteServer) error {
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
	s.logger.Info("[SERVER]", "Received a SendWrite request, sending the file for fileId:"+filename)

	memList := s.Node.GetMembershipList()

	for _, member := range memList.Member {
		if member.NodeId == s.Node.NodeId {
			if !utils.CheckInArray(filename, member.FileList) {
				member.FileList = append(member.FileList, filename)
				break
			}
		}
	}

	s.Node.PutMembershipList(memList)

	err := ioutil.WriteFile(s.Config.SdfsDir+"/"+filename, byteStringArray, 0644)
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

func (s *Server) RemoteRead(ctx context.Context, in *pb.RemoteReadRequest) (*pb.RemoteReadResponse, error) {

	filename := in.Filename
	sdfsname := in.Sdfsname
	s.logger.Info("[SERVER]", "Received a remote read request, file: "+filename+" and sdfs: "+sdfsname)

	//call n.Download here
	s.Node.Download(filename, sdfsname, s.Config, s.logger)

	status := "ok"
	resp := &pb.RemoteReadResponse{
		Status: status,
	}
	return resp, nil
}

func (s *Server) DeleteFile(ctx context.Context, in *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	filename := in.Filename
	s.logger.Info("[SERVER]", "Received a request to delete the replica, file on sdfs: "+filename)

	err := os.Remove(s.Config.SdfsDir + "/" + filename)
	if err != nil {
		return nil, err
	}

	memList := s.Node.GetMembershipList()

	for _, member := range memList.Member {
		if member.NodeId == s.Node.NodeId {
			member.FileList = utils.DeleteInArray(filename, member.FileList)
			member.LeaderList = utils.DeleteInArray(filename, member.LeaderList)
			break
		}
	}

	s.Node.PutMembershipList(memList)

	resp := &pb.DeleteFileResponse{
		Status: "ok",
	}
	return resp, nil

}

func (s *Server) Enqueue(ctx context.Context, in *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	filename := in.Filename
	op := in.Operation
	originNodeId := in.Nodeid

	s.logger.Info("[SERVER]", "received a request to leader to enqueue: "+filename)

	q := s.Node.LeaderList.Get(filename)
	q.Enqueue(op, originNodeId)
	s.Node.LeaderList.Set(filename, q)

	resp := &pb.EnqueueResponse{
		Status: "ok",
	}
	return resp, nil

}

func (s *Server) Dequeue(ctx context.Context, in *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	filename := in.Filename
	originNodeId := in.Nodeid

	s.logger.Info("[SERVER]", "received a request to leader to dequeue: "+filename)

	q := s.Node.LeaderList.Get(filename)
	q.DequeueID(originNodeId)
	s.Node.LeaderList.Set(filename, q)

	resp := &pb.DequeueResponse{
		Status: "ok",
	}
	return resp, nil

}

func (s *Server) TopPeek(ctx context.Context, in *pb.TopPeekRequest) (*pb.TopPeekResponse, error) {
	filename := in.Filename
	s.logger.Info("[SERVER]", "received a request to leader to peek: "+filename)

	q := s.Node.LeaderList.Get(filename)
	top := q.Peek()
	top2 := q.Peek2nd()

	resp := &pb.TopPeekResponse{
		Top:  top,
		Top2: top2,
	}
	return resp, nil
}
