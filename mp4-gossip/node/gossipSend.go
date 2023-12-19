package node

import (
	"context"
	"math/rand"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/utils"
	"net"
	//"strconv"
	"golang.org/x/exp/slices"
	"strings"
	"sync"
	"time"
)

func (n *Node) ClientImpl(wg *sync.WaitGroup, logger *logger.CustomLogger, config *config.Config, filelogger *filelogger.FileLogger) {
	// fmt.Println("Welcome to CS425 MP2, a gossip failure detector")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer wg.Done()
	go n.clock(ctx)

	go n.InputReader(config, logger, filelogger)
	go n.periodicFunction(ctx, config, logger, filelogger)
	go n.failClean(logger, config, filelogger)
	//go n.leaderListMonitor(config, logger, filelogger)
	for {
		select {
		case <-n.terminate:
			cancel()
			break
		}
	}
}

func (n *Node) Gossip(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	//filelogger.Info("[GOSSIP]", "About to gossip")
	information := n.GetMembershipList()

	memberList := information.Member
	informationNew := &utils.MembershipList{}
	informationNew.MessageType = information.MessageType
	informationNew.IsSus = information.IsSus
	informationNew.Version = information.Version
	//n.PrintMembers(logger, filelogger, 2)

	for _, memberEntry := range memberList {
		if strings.Contains(memberEntry.NodeId, config.MachineName) {
			memberEntry.Heartbeat = memberEntry.Heartbeat + 1
			n.reqTime <- 0
			memberEntry.Localtime = <-n.currTime
		}

		if memberEntry.Sus && information.MessageType == 1 {
			continue
		}
		informationNew.Member = append(informationNew.Member, memberEntry)
	}
	// information.Member = memberList

	n.PutMembershipList(information)
	//filelogger.Info("[GOSSIP]", "MemberList length: "+strconv.Itoa(len(memberList)))
	//customPrint(informationNew, filelogger, "GOSSIP")
	if len(memberList) <= config.Bb {
		go UDPsend(memberList, informationNew, config, logger, filelogger)
	} else {
		rand.Seed(time.Now().UnixNano())
		randomMachines := make([]*utils.GossipMember, 0)
		for len(randomMachines) < config.Bb {
			index := rand.Intn(len(memberList))
			if memberList[index].NodeId == n.NodeId {
				continue
			}
			if !slices.Contains(randomMachines, memberList[index]) {
				randomMachines = append(randomMachines, memberList[index])
			}
		}
		go UDPsend(randomMachines, informationNew, config, logger, filelogger)
	}
}

func UDPsend(sendList []*utils.GossipMember, payload *utils.MembershipList, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	//fmt.Println("[SEND] ",payload)
	for _, member := range sendList {
		hostname := member.NodeId

		strs := strings.Split(hostname, "#")
		if config.MachineName == strs[0] {
			continue
		}
		//filelogger.Info("[GOSSIP]", "Sending gossip to: "+strs[0])
		go UDPsendHandler(strs[0], payload, config, logger, filelogger)
	}
}

func UDPsendHandler(hostname string, payload *utils.MembershipList, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	ServerAddr, err := net.ResolveUDPAddr("udp", hostname+":"+config.Port)
	if err != nil {
		logger.Error("[GOSSIP]", "Error in resolving UDP addr: ", err)
		filelogger.Error("[GOSSIP]", "Error in resolving UDP addr: ", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, ServerAddr)

	if err != nil {
		logger.Error("[GOSSIP]", "Error in resolving UDP addr: ", err)
		filelogger.Error("[GOSSIP]", "Error in resolving UDP addr: ", err)
		return
	}
	utils.WriteClient(conn, payload, logger)
	//filelogger.Info("[GOSSIP]", "Gossip sent to:"+hostname)
	defer conn.Close()
}
