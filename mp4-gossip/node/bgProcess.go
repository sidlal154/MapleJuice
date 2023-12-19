package node

import (
	"context"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/utils"
	"strconv"
	"time"

	"regexp"

	"golang.org/x/exp/slices"
)

func (n *Node) periodicFunction(ctx context.Context, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	ticker := time.NewTicker(time.Duration(config.Tgossip) * time.Millisecond)
	//tickerFail := time.NewTicker(time.Duration(config.Tfail) * time.Millisecond)
	//tickerCleanup := time.NewTicker(time.Duration(config.Tcleanup) * time.Millisecond)
	defer ticker.Stop()
	//defer tickerFail.Stop()
	//defer tickerCleanup.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			go n.Gossip(config, logger, filelogger)
			// case <-tickerFail.C:
			// 	go n.FailCheck(config, logger, filelogger)
			// case <-tickerCleanup.C:
			// 	go n.CleanupCheck(config, logger, filelogger)

		}
	}
}

func (n *Node) FailCheck(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	//filelogger.Info("[CLIENT]", "checking for failures")
	n.reqTime <- 0
	time := <-n.currTime
	information := n.GetMembershipList()
	memberList := information.Member
	for _, memberEntry := range memberList {
		if time-memberEntry.Localtime >= int64(config.Tfail) {
			memberEntry.Sus = true
			if information.MessageType == 3 {
				logger.Info("[NEW-SUSPECT]", "Node ID: "+memberEntry.NodeId+" Incarnation: "+strconv.FormatInt(memberEntry.Incarno, 10))
				//filelogger.Info("[NEW-SUSPECT]", "Node ID: "+memberEntry.NodeId+" Incarnation: "+strconv.FormatInt(memberEntry.Incarno, 10))
			} else if information.MessageType == 1 {
				logger.Info("[NEW-FAILURE]", "Node ID: "+memberEntry.NodeId)
				//filelogger.Info("[NEW-FAILURE]", "Node ID: "+memberEntry.NodeId)
			}
		}
	}
	n.PutMembershipList(information)
	//n.PrintMembers(logger, filelogger, 2)
}

func (n *Node) CleanupCheck(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	//filelogger.Info("[CLIENT]", "checking for cleanup")
	n.reqTime <- 0
	time := <-n.currTime
	information := n.GetMembershipList()
	memberList := information.Member
	var eraseNodes []string
	for _, memberEntry := range memberList {
		if time-memberEntry.Localtime >= int64(config.Tcleanup)+int64(config.Tcleanup) {
			if memberEntry.Sus == true {
				n.failStop = true
				eraseNodes = append(eraseNodes, memberEntry.NodeId)
				logger.Info("[CLEAN]", "Cleaned up node: "+memberEntry.NodeId)
				//filelogger.Info("[CLEAN]", "Cleaned up node: "+memberEntry.NodeId)
			}
		}
	}
	if information.MessageType == 3 {
		n.counterList = append(n.counterList, eraseNodes...)
	}
	newMemList := n.Delete(eraseNodes, memberList)
	information.Member = newMemList
	n.PutMembershipList(information)
	//n.PrintMembers(logger, filelogger, 2)
}

func (n *Node) Delete(name []string, memberList []*utils.GossipMember) []*utils.GossipMember {
	var updatedList []*utils.GossipMember
	for _, member := range memberList {
		if !slices.Contains(name, member.NodeId) {
			updatedList = append(updatedList, member)
		}
	}
	return updatedList
}

func (n *Node) clock(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.reqTime:
			elapsed := time.Since(startTime).Milliseconds()
			n.currTime <- elapsed
		}
	}
}

func (n *Node) failClean(logger *logger.CustomLogger, config *config.Config, filelogger *filelogger.FileLogger) {
	for {
		var eraseNodes []string
		n.reqTime <- 0
		time := <-n.currTime
		information := n.GetMembershipList()
		memberList := information.Member
		//mp4 changes start
		re := regexp.MustCompile(`(\d{2})\.cs`)
		highestRep := make(map[string]string)
		fileReplicas := make(map[string][]string)
		count := make(map[string]int)
		//mp4 changes end

		for _, memberEntry := range memberList {
			if time-memberEntry.Localtime >= int64(config.Tfail) {
				if memberEntry.Sus != true && !slices.Contains(n.counterList, memberEntry.NodeId) {
					memberEntry.Sus = true
					if information.MessageType == 3 {
						logger.Info("[NEW-SUSPECT]", "Node ID: "+memberEntry.NodeId+" Incarnation: "+strconv.FormatInt(memberEntry.Incarno, 10))
						//filelogger.Info("[NEW-SUSPECT]", "Node ID: "+memberEntry.NodeId+" Incarnation: "+strconv.FormatInt(memberEntry.Incarno, 10))
					} else if information.MessageType == 1 {
						//logger.Info("[NEW-POSSIBLE-FAILURE]", "Node ID: "+memberEntry.NodeId)
						//filelogger.Info("[NEW-FAILURE]", "Node ID: "+memberEntry.NodeId)
					}
				}
			}
			if time-memberEntry.Localtime >= int64(config.Tcleanup)+int64(config.Tcleanup) {
				if memberEntry.Sus == true && !slices.Contains(n.counterList, memberEntry.NodeId) {
					n.failStop = true
					eraseNodes = append(eraseNodes, memberEntry.NodeId)
					logger.Info("[CLEAN-FAILURE]", "Cleaned up node: "+memberEntry.NodeId)

					//mp3 change starts
					for _, file := range memberEntry.LeaderList {
						logger.Info("[DEBUG]", "Starting leaderElection for file: "+file)
						fileReplicas[file] = append(fileReplicas[file], memberEntry.NodeId)
						for _, member := range memberList {
							if member.NodeId != memberEntry.NodeId && slices.Contains(member.FileList, file) {
								logger.Info("[DEBUG]", "Can this member be a leader?: "+member.NodeId)
								if member.NodeId != n.NodeId {
									fileReplicas[file] = append(fileReplicas[file], member.NodeId)
								}
								match := re.FindStringSubmatch(member.NodeId)
								if len(match) > 1 {
									result := match[1]
									num, _ := strconv.Atoi(result)
									value, exists := highestRep[file]
									if !exists {
										highestRep[file] = member.NodeId
									} else {
										prev := re.FindStringSubmatch(value)
										prev_num, _ := strconv.Atoi(prev[1])
										if num > prev_num {
											highestRep[file] = member.NodeId
										}
									}
								}
							}
						}
						logger.Info("[DEBUG]", "Leader Elected for file: "+file+" : "+highestRep[file])
					}
					//mp3 change ends

					for _, file := range memberEntry.FileList {
						_, exists := highestRep[file]
						if n.CheckLeader(file) == "NONE" && !exists {
							logger.Info("[DEBUG]", "Starting leaderElection for file: "+file)
							fileReplicas[file] = append(fileReplicas[file], memberEntry.NodeId)
							for _, member := range memberList {
								if member.NodeId != memberEntry.NodeId && slices.Contains(member.FileList, file) {
									logger.Info("[DEBUG]", "Can this member be a leader?: "+member.NodeId)
									if member.NodeId != n.NodeId {
										fileReplicas[file] = append(fileReplicas[file], member.NodeId)
									}
									match := re.FindStringSubmatch(member.NodeId)
									if len(match) > 1 {
										result := match[1]
										num, _ := strconv.Atoi(result)
										value, exists := highestRep[file]
										if !exists {
											highestRep[file] = member.NodeId
										} else {
											prev := re.FindStringSubmatch(value)
											prev_num, _ := strconv.Atoi(prev[1])
											if num > prev_num {
												highestRep[file] = member.NodeId
											}
										}
									}
								}
							}
							logger.Info("[DEBUG]", "Leader Elected for file: "+file+" : "+highestRep[file])
						}
					}

					//filelogger.Info("[CLEAN]", "Cleaned up node: "+memberEntry.NodeId)
				}
			}
		}
		//mp3 start
		if len(eraseNodes) > 0 {
			for key := range highestRep {
				if highestRep[key] == n.NodeId {
					//set the leader
					logger.Info("[DEBUG]", "I am the leader of: "+key)
					n.replicaTracker[key] = fileReplicas[key]
					count[key] = config.Replica - len(n.replicaTracker[key])
					n.LeaderList.Set(key, &Queue{})
					for _, member := range memberList {
						if member.NodeId == n.NodeId {
							member.LeaderList = append(member.LeaderList, key)
							break
						}
					}
				}
			}
		}
		//mp3 end
		// if information.MessageType == 3 {
		n.counterList = append(n.counterList, eraseNodes...)
		// }
		newMemList := n.Delete(eraseNodes, memberList)
		information.Member = newMemList
		n.PutMembershipList(information)
		//n.PrintMembers(logger, filelogger, 2)
		//mp3 start
		for _, entry := range eraseNodes {
			for _, queue := range n.LeaderList.data {
				queue.DequeueAll(entry)
			}
			for file, replicaList := range n.replicaTracker {
				if slices.Contains(replicaList, entry) {
					n.Duplicate(file, entry, config, logger, count[file])
				}
			}
		}

		//mp3 end

		//TODO: for all the failed members, check which tasks were pending, and reschedule them, this will be done only on the leader
		//TODO: delete the entries of these processes from the tracker

		memListIndex := 0
		for _, entry := range eraseNodes {

			mapleTracker := n.GetMapleTracker()
			tasks := mapleTracker[entry]
			anychanges := false
			for _, task := range tasks {
				if task.Status == "ongoing" {
					if newMemList[memListIndex].Sus {
						memListIndex++
					}
					if memListIndex == len(newMemList) {
						memListIndex = 0
					}
					memberId := newMemList[memListIndex].NodeId
					n.RescheduleMapleTask(config, logger, memberId, task)
					task.Status = "deleted"
					anychanges = true
				}
			}
			var filteredMapleTasks []*utils.TaskTracker
			for _, task := range tasks {
				if task.Status != "deleted" && anychanges {
					filteredMapleTasks = append(filteredMapleTasks, task)
				}
			}
			mapleTracker[entry] = filteredMapleTasks
			n.PutMapleTracker(mapleTracker)

			juiceTracker := n.GetJuiceTracker()
			Jtasks := juiceTracker[entry]
			anychanges = false
			for _, task := range Jtasks {
				if task.Status == "ongoing" {
					if newMemList[memListIndex].Sus {
						memListIndex++
					}
					if memListIndex == len(newMemList) {
						memListIndex = 0
					}
					memberId := newMemList[memListIndex].NodeId
					n.RescheduleJuiceTask(config, logger, memberId, task)
					task.Status = "deleted"
					anychanges = true
				}
			}
			var filteredJuiceTasks []*utils.TaskTracker
			for _, task := range tasks {
				if task.Status != "deleted" {
					filteredJuiceTasks = append(filteredJuiceTasks, task)
				}
			}
			juiceTracker[entry] = filteredJuiceTasks
			n.PutJuiceTracker(juiceTracker)

		}

	}
}

// ===========================================================
