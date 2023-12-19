package node

import (
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
)

func (n *Node) ActiveFiles(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	information := n.GetMembershipList()
	memList := information.Member
	logger.Info("[STORE]", "Files present in current node:")
	for _, member := range memList {
		if member.NodeId == n.NodeId {
			for _, file := range member.FileList {
				logger.Info("[STORE]", file)
			}
		}
	}
}
