package node

import (
	"golang.org/x/exp/slices"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"strings"
)

func (n *Node) ListLocations(sdfsName string, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	information := n.GetMembershipList()
	memList := information.Member
	logger.Info("[LS]", "File is present at:")
	for _, member := range memList {
		if slices.Contains(member.FileList, sdfsName) {
			logger.Info("[LS]", member.NodeId)
		}
	}
}

func (n *Node) ListStoredReplicas(logger *logger.CustomLogger) {
	info := n.GetMembershipList()
	memList := info.Member
	logger.Info("[STORE]", "Replicas present on this peer:")
	for _, member := range memList {
		if member.NodeId == n.NodeId {
			leaderList := member.LeaderList
			logger.Info("[STORE]", strings.Join(leaderList, ","))
			break
		}
	}

}
