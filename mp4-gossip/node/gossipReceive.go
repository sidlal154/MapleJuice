package node

import (
	"golang.org/x/exp/slices"
	"math/rand"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/utils"
	"net"
	"strconv"
	"sync"
	"time"
)

func (n *Node) ServerImpl(wg *sync.WaitGroup, logger *logger.CustomLogger, config *config.Config, filelogger *filelogger.FileLogger) {
	defer wg.Done()
	for {
		//filelogger.Info("[MAIN]", "Starting Server thread")
		req, UDPAddr, err := utils.Read(n.Listener, logger)

		if err != nil {
			logger.Error("[MAIN]", "Some error while reading: ", err)
			//filelogger.Error("[MAIN]", "Some error while reading: ", err)
			continue
		}
		//fmt.Println(string(bytes))
		if err != nil {
			logger.Error("[MAIN]", "unmarshaling error: ", err)
			//filelogger.Error("[MAIN]", "unmarshaling error: ", err)
			return
		}

		if req.MessageType == 2 {
			go n.HandleIntroducer(req, UDPAddr, config, logger, filelogger)
		} else {
			go n.HandleGossip(req, UDPAddr, config, logger, filelogger)
		}
	}
}

func (n *Node) HandleGossip(req *utils.MembershipList, addr *net.UDPAddr, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	//filelogger.Info("[NODE]", "New Gossip message received from "+addr.String())

	inMemeory := n.GetMembershipList()

	rand.Seed(time.Now().UnixNano())
	lottery := rand.Intn(100)

	if lottery < config.DropRate {
		//filelogger.Info("[NODE]", "Dropping this gossip....")
		return
	}

	incomingMemList := req.Member
	if n.Version < req.Version {
		if inMemeory.MessageType == 1 && req.MessageType == 3 {
			logger.Info("[MODE SWITCH]", "Switching to GOSSIP+SUSPICION mode")
			//filelogger.Info("[MODE SWITCH]", "Switching to GOSSIP+SUSPICION mode")
		}
		if inMemeory.MessageType == 3 && req.MessageType == 1 {
			logger.Info("[MODE SWITCH]", "Switching to GOSSIP mode")
			//filelogger.Info("[MODE SWITCH]", "Switching to GOSSIP mode")
		}
		inMemeory.MessageType = req.MessageType
	}

	for _, incomingMem := range incomingMemList {
		checker := utils.CheckMember(incomingMem.NodeId, inMemeory.Member)
		if checker != nil {
			if inMemeory.MessageType == 1 {
				if incomingMem.Heartbeat > checker.Heartbeat {
					n.reqTime <- 0
					checker.Localtime = <-n.currTime
					checker.Heartbeat = incomingMem.Heartbeat
					//mp4: changed here
					//TODO: update carefully, no ghost entries
					checker.FileList = incomingMem.FileList
					checker.LeaderList = incomingMem.LeaderList
					if checker.Sus == true {
						checker.Sus = false
					}
				}
			} else if inMemeory.MessageType == 3 {

				if incomingMem.NodeId == n.NodeId && incomingMem.Sus == true {
					checker.Sus = false
					if checker.Incarno == incomingMem.Incarno {
						checker.Incarno = checker.Incarno + 1
						logger.Info("[SUSPECTED]", "INCERASED my Incarnation number to: "+strconv.Itoa(int(checker.Incarno)))
						//filelogger.Info("[SUSPECTED]", "INCERASED my Incarnation number to: "+strconv.Itoa(int(checker.Incarno)))
					}
					n.MyIncarnation = int(checker.Incarno)
				} else {
					if incomingMem.Heartbeat > checker.Heartbeat {
						n.reqTime <- 0
						checker.Localtime = <-n.currTime
						checker.Heartbeat = incomingMem.Heartbeat
					}
					if incomingMem.Incarno > checker.Incarno {
						checker.Sus = incomingMem.Sus
						checker.Incarno = incomingMem.Incarno
						checker.Heartbeat = incomingMem.Heartbeat
						n.reqTime <- 0
						checker.Localtime = <-n.currTime
					} else if incomingMem.Incarno == checker.Incarno {
						if incomingMem.Sus == true {
							checker.Sus = incomingMem.Sus
							logger.Info("[SUSPECTED]", "NODE: "+checker.NodeId+" Incarnation: "+strconv.Itoa(int(checker.Incarno)))
							//filelogger.Info("[SUSPECTED]", "NODE: "+checker.NodeId+" Incarnation: "+strconv.Itoa(int(checker.Incarno)))
						}
					}
				}

			}
		} else {
			if slices.Contains(n.counterList, incomingMem.NodeId) && inMemeory.MessageType == 3 {
				continue
			}
			n.reqTime <- 0
			time := <-n.currTime
			newMember := &utils.GossipMember{
				NodeId:    incomingMem.NodeId,
				Heartbeat: incomingMem.Heartbeat,
				Localtime: time,
				Sus:       incomingMem.Sus,
				Incarno:   0,
			}
			inMemeory.Member = append(inMemeory.Member, newMember)
		}
	}
	n.PutMembershipList(inMemeory)
	//n.PrintMembers(logger, filelogger, 2)
	//are we explicitly calling? Or let the client handle it? -- both could be done, in that case once server triggers and after Tgossip client calls one
}
