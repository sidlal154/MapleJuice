package node

import (
	"io/ioutil"
	"mp4-gossip/config"
	"mp4-gossip/logger"
	pb "mp4-gossip/protos/grpcServer"
	"strconv"
	"strings"
	"time"
)

func (n *Node) Download(file string, sdfsName string, config *config.Config, logger *logger.CustomLogger) {

	//TODO: Check if there is a leader elected, if no leader is elected, wait. If a leader is elected, we get the queue from it, see our status, and then proceed appropriately

	//Check if you can read right now from the queue

	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	logger.Info("[DOWNLOAD]", "Starting a get request")

	for {
		leader := n.CheckLeader(sdfsName)
		if leader != "NONE" {
			req1 := &pb.EnqueueRequest{
				Filename:  sdfsName,
				Operation: "Read",
				Nodeid:    n.NodeId,
			}

			_, err := n.GrpcClient.ClientEnqueue(leader, req1)
			if err != nil {
				logger.Error("[DOWNLOAD]", "Some error while enqueueing: ", err)
				time.Sleep(1 * time.Second)
				continue
			}

			t := 0
			for {
				req2 := &pb.TopPeekRequest{
					Filename: sdfsName,
				}
				resp, err := n.GrpcClient.ClientTopPeek(leader, req2)
				if err != nil {
					logger.Error("[DOWNLOAD]", "Some error while peeking: ", err)
					//break then continue
					t = 1
					break
				}
				top := resp.Top
				top2 := resp.Top2
				if top == n.NodeId || top2 == n.NodeId {
					break
				}
				time.Sleep(1 * time.Second)
			}

			if t == 1 {
				time.Sleep(1 * time.Second)
				continue
			}

			logger.Info("[DOWNLOAD]", "Started a get request")

			req2 := &pb.GetReadFileRequest{
				Filename: sdfsName,
			}
			resp, err := n.GrpcClient.ClientGetReadFile(leader, req2)
			if err == nil {
				err = ioutil.WriteFile(config.LocalDir+"/"+file, resp.Data, 0644)
				if err != nil {
					logger.Error("[DOWNLOAD]", "Some error while downloading file, retrying on new leader....", err)
					continue
				}
				logger.Info("[ACKNOWLEDGEMENT]", "Get successful")
				req3 := &pb.DequeueRequest{
					Filename: sdfsName,
					Nodeid:   n.NodeId,
				}
				_, err = n.GrpcClient.ClientDequeue(leader, req3)
				break
			} else {
				logger.Info("[DOWNLOAD]", "Seems like leader died, Trying on new leader node")
			}
		}
		time.Sleep(1 * time.Second)
	}

	// req := &pb.SendReadRequest{
	// 	Filename: sdfsName,
	// }
	// resp, err := n.GrpcClient.ClientSendRead("localhost",req)
	// if err!=nil {
	// 	logger.Error("[DOWNLOAD]","Some error while getting list of peers with file: ",err)
	// 	return
	// }
	// //destinationId
	// peers := strings.Split(resp.DestinationId,",")

	// //Read from one of these peer nodes starting from the leader
	// req2 := &pb.GetReadFileRequest{
	// 	Filename: sdfsName,
	// }

	// for _ ,peer := range peers {
	// 	resp, err := n.GrpcClient.ClientGetReadFile(peer,req2)
	// 	if err ==  nil {
	// 		err = ioutil.WriteFile(config.LocalDir+"/"+file,resp.Data,0644)
	// 		if err!=nil {
	// 			logger.Error("[DOWNLOAD]","Some error while downloading file, retrying....",err)
	// 			continue
	// 		}
	// 		logger.Info("[ACKNOWLEDGEMENT]","Get successful")
	// 		break
	// 	} else {
	// 		logger.Info("[DOWNLOAD]","Trying on new replica node")
	// 	}
	// }
	// q.DequeueID(n.NodeId)
	// n.leaderList.Set(sdfsName,q)
	endTime := time.Now().UnixNano() / int64(time.Millisecond)

	logger.Info("[TIME-DOWNLOAD]", "Total time taken for Download: "+strconv.FormatInt(endTime-startTime, 10))
}

func (n *Node) MultiDownload(file string, sdfsName string, ListPeersString string, config *config.Config, logger *logger.CustomLogger) {
	/*
		Create a grpc call which calls n.Download() remotely when called. This should be done in goroutines independently
	*/
	ListPeers := strings.Split(ListPeersString, ",")
	req := &pb.RemoteReadRequest{
		Filename: file,
		Sdfsname: sdfsName,
	}
	for _, peer := range ListPeers {
		go func(p string) {
			resp, err := n.GrpcClient.ClientRemoteRead(p, req)
			if err != nil {
				logger.Error("[MULTI-DOWNLOAD]", "Some error while getting list of peers with file: ", err)
				return
			}
			logger.Info("[MULTI-DOWNLOAD]", "status: "+resp.Status)
		}(peer)
	}

}
