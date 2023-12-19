package node

import (
	"mp4-gossip/config"
	"mp4-gossip/logger"
	pb "mp4-gossip/protos/grpcServer"
	"strings"
	//"fmt"
	"mp4-gossip/utils"
)

func (n *Node) FileDelete(filename string, config *config.Config, logger *logger.CustomLogger) {
	memList := n.GetMembershipList()
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
	membersList := memList.Member
	for _, member := range membersList {
		if utils.CheckInArray(filename, member.FileList) && member.NodeId != leader {
			peers = append(peers, strings.Split(member.NodeId, "#")[0])
		}
	}

	req := &pb.DeleteFileRequest{
		Filename: filename,
	}

	for _, peer := range peers {
		go func(p string) {
			resp, err := n.GrpcClient.ClientDeleteFile(p, req)
			if err != nil {
				logger.Error("[DELETE]", "Some error while getting list of peers with file: ", err)
				return
			}
			logger.Info("[DELETE]", "status: "+resp.Status)
		}(peer)
	}

	logger.Info("[ACKNOWLEDGE]", "Delete successful")
}
