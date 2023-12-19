package node

import (
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/utils"
	"strconv"
	"strings"
)

func (n *Node) PrintMembers(logger *logger.CustomLogger, filelogger *filelogger.FileLogger, mode int) {
	information := n.GetMembershipList()
	memList := information.Member
	//1 - CONSOLE, 2 - FILE, 3 - BOTH
	if mode == 1 {
		if information.MessageType == 3 {
			logger.Info("[MODE]", "GOSSIP+SUSPICION")
		}
		if information.MessageType == 1 {
			logger.Info("[MODE]", "GOSSIP")
		}
		for _, member := range memList {
			if information.MessageType == 3 {
				logger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|"+strconv.FormatInt(member.Incarno, 10)+"| SUSPECTED: "+strconv.FormatBool(member.Sus))
			} else {
				logger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|FAILED: "+strconv.FormatBool(member.Sus)+"|FileList :["+strings.Join(member.FileList, ",")+"]|LeaderList: ["+strings.Join(member.LeaderList, ",")+"]")
			}
		}
	} else if mode == 2 {
		if information.MessageType == 3 {
			filelogger.Info("[MODE]", "GOSSIP+SUSPICION")
		}
		if information.MessageType == 1 {
			filelogger.Info("[MODE]", "GOSSIP")
		}
		for _, member := range memList {
			if information.MessageType == 3 {
				filelogger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|"+strconv.FormatInt(member.Incarno, 10)+"| SUSPECTED: "+strconv.FormatBool(member.Sus))
			} else {
				filelogger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|FAILED: "+strconv.FormatBool(member.Sus))
			}
		}
		if information.MessageType == 3 {
			for _, machineId := range n.counterList {
				filelogger.Info("[COUNTER-LIST]", machineId)
			}
		}
	} else if mode == 3 {
		if information.MessageType == 3 {
			logger.Info("[MODE]", "GOSSIP+SUSPICION")
			filelogger.Info("[MODE]", "GOSSIP+SUSPICION")
		}
		if information.MessageType == 1 {
			logger.Info("[MODE]", "GOSSIP")
			filelogger.Info("[MODE]", "GOSSIP")
		}
		for _, member := range memList {
			if information.MessageType == 3 {
				logger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|"+strconv.FormatInt(member.Incarno, 10)+"| SUSPECTED: "+strconv.FormatBool(member.Sus))
				filelogger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|"+strconv.FormatInt(member.Incarno, 10)+"| SUSPECTED: "+strconv.FormatBool(member.Sus))
			} else {
				logger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|FAILED: "+strconv.FormatBool(member.Sus))
				filelogger.Info("[MEM-LIST]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|FAILED: "+strconv.FormatBool(member.Sus))
			}
		}
		if information.MessageType == 3 {
			for _, machineId := range n.counterList {
				filelogger.Info("[COUNTER-LIST]", machineId)
			}
		}
	}
}

func customPrint(information *utils.MembershipList, filelogger *filelogger.FileLogger, tag string) {
	for _, member := range information.Member {
		filelogger.Info("["+tag+"]", member.NodeId+"|"+strconv.FormatInt(member.Heartbeat, 10)+"|"+strconv.FormatInt(member.Localtime, 10)+"|"+strconv.FormatInt(member.Incarno, 10)+"| SUSPECTED: "+strconv.FormatBool(member.Sus))
	}
	filelogger.Info("["+tag+"]", strconv.FormatInt(information.MessageType, 10))
	filelogger.Info("["+tag+"]", strconv.FormatBool(information.IsSus))
}
