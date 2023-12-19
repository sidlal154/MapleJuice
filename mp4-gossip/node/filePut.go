package node

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"mp4-gossip/config"
	"mp4-gossip/logger"
	pb "mp4-gossip/protos/grpcServer"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

// func (n *Node) Upload(file string, sdfsName string, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
func (n *Node) Upload(file string, sdfsName string, config *config.Config, logger *logger.CustomLogger) {
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	//the notion of file for first time and update changes: first time: contains a path || second time: contains sdfsName ????
	if !n.CheckFile(sdfsName) {
		hashedName := sha256.Sum256([]byte(file))
		hashedNameHex := hex.EncodeToString(hashedName[:])

		logger.Info("[UPLOAD]", "Hashed Name:"+hashedNameHex)
		logger.Info("[UPLOAD]", "Started a fresh put request")

		decimalValue, err := hex.DecodeString(hashedNameHex)
		if err != nil {
			fmt.Println("Error converting hex to decimal")
			return
		}
		lastByte := decimalValue[len(decimalValue)-1]
		truncatedValue := int(lastByte) & ((1 << 4) - 1)
		nodeValue := truncatedValue % 10
		logger.Info("[UPLOAD]", "Decimal Value:"+strconv.Itoa(truncatedValue))
		nodeList := n.ReplicaList(nodeValue, config, logger)
		fmt.Println(nodeList)
		//When a node uploads a file it becomes its own leader
		// n.leaderChannel[sdfsName] = make(chan int)
		// n.leaderList[sdfsName] = NewRWMaintain(n.leaderChannel[sdfsName], config, logger, filelogger)
		n.LeaderList.Set(sdfsName, &Queue{})
		n.replicaTracker[sdfsName] = nodeList
		//n.addCh <- sdfsName

		byteStringArray, err := ioutil.ReadFile(config.LocalDir + "/" + file)
		if err != nil {
			logger.Error("[UPLOAD]", "Some error while reading file", err)
			return
		}

		err = ioutil.WriteFile(config.SdfsDir+"/"+sdfsName, byteStringArray, 0644)
		if err != nil {
			logger.Error("[UPLOAD]", "Some error while reading file", err)
			return
		}
		memList := n.GetMembershipList()
		for _, member := range memList.Member {
			if member.NodeId == n.NodeId {
				member.FileList = append(member.FileList, sdfsName)
				member.LeaderList = append(member.LeaderList, sdfsName)
				break
			}
		}

		n.PutMembershipList(memList)

		flag := 0
		for _, node := range nodeList {

			req := &pb.SendWriteRequest{
				Filename: sdfsName,
				Data:     byteStringArray,
			}
			nodeAddress := strings.Split(node, "#")[0]
			resp, err := n.GrpcClient.ClientSendWrite(nodeAddress, req)
			if err != nil {
				logger.Info("[UPLOAD-FRESH]", "No response from SendWriteRequest")
			} else {
				logger.Info("[UPLOAD-FRESH]", "Response Received: "+resp.Status)
				flag = 1
			}
		}

		if flag == 0 {
			logger.Info("[UPLOAD-FRESH]", "Couldn't upload to any machines")
		} else {
			logger.Info("[ACKNOWLEDGEMENT]", "Fresh Put command successful")
		}

	} else {
		/*
			THis is an update, find where the replicas are and do the same thing for this
		*/

		//TODO: Check if there is a leader elected, if no leader is elected, wait. If a leader is elected, we get the queue from it, see our status, and then proceed appropriately
		// q := n.leaderList.Get(sdfsName)

		// q.Enqueue("Write",n.NodeId)
		// n.leaderList.Set(sdfsName,q)

		logger.Info("[UPLOAD-UPDATE]", "Starting a update put request")

		for {
			leader := n.CheckLeader(sdfsName)
			if leader != "NONE" {
				req1 := &pb.EnqueueRequest{
					Filename:  sdfsName,
					Operation: "Write",
					Nodeid:    n.NodeId,
				}

				_, err := n.GrpcClient.ClientEnqueue(leader, req1)
				if err != nil {
					logger.Error("[UPLOAD-UPDATE]", "Some error while enqueueing: ", err)
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
						logger.Error("[UPLOAD-UPDATE]", "Some error while peeking: ", err)
						//break then continue
						t = 1
						break
					}
					top := resp.Top
					logger.Info("[UPLOAD-UPDATE]", "Top of queue: "+top)
					if top == n.NodeId {
						break
					}
					time.Sleep(1 * time.Second)
				}

				if t == 1 {
					time.Sleep(1 * time.Second)
					continue
				}

				logger.Info("[UPLOAD-UPDATE]", "Started a update put request")

				req := &pb.SendReadRequest{
					Filename: sdfsName,
				}
				resp, err := n.GrpcClient.ClientSendRead("localhost", req)
				if err != nil {
					logger.Error("[UPLOAD-UPDATE]", "Some error while getting list of peers with file: ", err)
					return
				}

				nodeList := strings.Split(resp.DestinationId, ",")

				byteStringArray, err := ioutil.ReadFile(config.LocalDir + "/" + file)
				if err != nil {
					logger.Error("[UPLOAD-UPDATE]", "Some error while reading file", err)
					return
				}

				flag := 0

				for _, node := range nodeList {

					req := &pb.SendWriteRequest{
						Filename: sdfsName,
						Data:     byteStringArray,
					}
					nodeAddress := strings.Split(node, "#")[0]
					resp, err := n.GrpcClient.ClientSendWrite(nodeAddress, req)
					if err != nil {
						logger.Error("[UPLOAD-UPDATE]", "Error, couldn't write an instance of replica", err)
					} else {
						logger.Info("[UPLOAD-UPDATE]", "Response Received, 1 replica written: "+resp.Status)
						flag = 1
					}
				}

				if flag == 0 {
					logger.Info("[UPLOAD-UPDATE]", "Couldn't upload to any machines")
				} else {
					logger.Info("[ACKNOWLEDGEMENT]", "Update Put command successful")
					req3 := &pb.DequeueRequest{
						Filename: sdfsName,
						Nodeid:   n.NodeId,
					}
					_, err = n.GrpcClient.ClientDequeue(leader, req3)
					break
				}
			}
			time.Sleep(1 * time.Second)
		}

		// for {
		// 	top := q.Peek()
		// 	if top == n.NodeId {
		// 		break;
		// 	}
		// 	time.Sleep(1*time.Second)
		// }

		// req := &pb.SendReadRequest{
		// 	Filename: sdfsName,
		// }
		// resp, err := n.GrpcClient.ClientSendRead("localhost",req)
		// if err!=nil {
		// 	logger.Error("[UPLOAD-UPDATE]","Some error while getting list of peers with file: ",err)
		// 	return
		// }

		// nodeList := strings.Split(resp.DestinationId,",")

		// byteStringArray, err := ioutil.ReadFile(config.LocalDir + "/"+file)
		// if err!= nil{
		// 	logger.Error("[UPLOAD-UPDATE]","Some error while reading file",err)
		// 	return
		// }

		// flag := 0

		// for _,node := range nodeList {

		// 	req := &pb.SendWriteRequest{
		// 		Filename: sdfsName,
		// 		Data: byteStringArray,
		// 	}
		// 	nodeAddress := strings.Split(node,"#")[0]
		// 	resp, err := n.GrpcClient.ClientSendWrite(nodeAddress,req)
		// 	if err!=nil {
		// 		logger.Info("[UPLOAD-UPDATE]","No response from SendWriteRequest")
		// 	} else {
		// 		logger.Info("[UPLOAD-UPDATE]","Response Received: "+resp.Status)
		// 		flag = 1
		// 	}
		// }

		// if flag == 0 {
		// 	logger.Info("[UPLOAD-UPDATE]","Couldn't upload to any machines")
		// } else {
		// 	logger.Info("[ACKNOWLEDGEMENT]","Update Put command successful")
		// }

		// q.DequeueID(n.NodeId)
		// n.leaderList.Set(sdfsName,q)
	}

	endTime := time.Now().UnixNano() / int64(time.Millisecond)

	logger.Info("[TIME-UPLOAD]", "Total time taken for Upload: "+strconv.FormatInt(endTime-startTime, 10))
}

func (n *Node) ReplicaList(value int, config *config.Config, logger *logger.CustomLogger) []string {
	var nodeList []string
	information := n.GetMembershipList()
	memList := information.Member
	rand.Seed(time.Now().UnixNano())
	if len(memList) < config.Replica {
		for _, mem := range memList {
			if mem.NodeId == n.NodeId {
				continue
			}
			nodeList = append(nodeList, mem.NodeId)
		}
	} else {
		for len(nodeList) < config.Replica-1 {
			index := rand.Intn(len(memList))
			//added .Sus to prevent replica from being stored to a suspected node
			if slices.Contains(nodeList, memList[index].NodeId) || memList[index].Sus || memList[index].NodeId == n.NodeId {
				continue
			}
			nodeList = append(nodeList, memList[index].NodeId)
		}
	}
	return nodeList
}

func (n *Node) CheckFile(sdfsName string) bool {
	information := n.GetMembershipList()
	memList := information.Member
	for _, member := range memList {
		if slices.Contains(member.FileList, sdfsName) {
			return true
		}
	}
	return false
}

func (n *Node) Duplicate(sdfsName string, node string, config *config.Config, logger *logger.CustomLogger, reqReplicas int) {
	//node is the failed node
	logger.Info("[DUPLICATE]", "Checking if I need to create extra replicas, reqReplicas: ")
	fmt.Println("reqREplicas: ", reqReplicas)
	if reqReplicas == 0 {
		reqReplicas = 1
	}
	information := n.GetMembershipList()
	memList := information.Member
	if len(n.replicaTracker[sdfsName])-1 >= len(memList) {
		return
	} else {
		flag := 0
		for _, member := range memList {
			rt, _ := n.replicaTracker[sdfsName]
			if member.NodeId != node && !slices.Contains(member.FileList, sdfsName) && !slices.Contains(rt, member.NodeId) {
				//call GRPC with that node
				byteStringArray, err := ioutil.ReadFile(config.SdfsDir + "/" + sdfsName)
				if err != nil {
					logger.Error("[DUPLICATE]", "Some error while reading file", err)
					return
				}
				req := &pb.SendWriteRequest{
					Filename: sdfsName,
					Data:     byteStringArray,
				}
				nodeAddress := strings.Split(member.NodeId, "#")[0]
				resp, err := n.GrpcClient.ClientSendWrite(nodeAddress, req)

				if err != nil {
					logger.Error("[DUPLICATE]", "Error, couldn't write an instance of extra replica", err)
				} else {
					logger.Info("[DUPLICATE]", "Extra Response Received, 1 replica written: "+resp.Status)
					flag = flag + 1
				}
				for i := 0; i < len(n.replicaTracker[sdfsName]); i++ {
					if n.replicaTracker[sdfsName][i] == node {
						n.replicaTracker[sdfsName][i] = member.NodeId
					}
				}
				if flag == reqReplicas {
					break
				}

			}
		}
	}
}
