package node

import (
	"bufio"
	"fmt"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"os"
	"strconv"
	"strings"
	"syscall"
	//"time"
)

// NO comma for JOIN query

func (n *Node) InputReader(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	fmt.Println("ENTER YOUR COMMAND:")
	for {
		scanner := bufio.NewScanner(os.Stdin)
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		if line == "DISABLE_SUSPICION" {
			information := n.GetMembershipList()
			if information.MessageType == 1 {
				fmt.Println("Already in Gossip mode")
			} else {
				information.MessageType = 1
				n.Version = n.Version + 1
				information.Version = information.Version + 1
				logger.Info("[MODE SWITCH]", "Switching to GOSSIP mode")
				//filelogger.Info("[MODE SWITCH]", "Switching to GOSSIP mode")
				n.PutMembershipList(information)
			}
		} else if line == "ENABLE_SUSPICION" {
			information := n.GetMembershipList()
			if information.MessageType == 3 {
				fmt.Println("Already in Gossip+S mode")
			} else {
				information.MessageType = 3
				n.Version = n.Version + 1
				information.Version = information.Version + 1
				logger.Info("[MODE SWITCH]", "Switching to GOSSIP+SUSPICION mode")
				//filelogger.Info("[MODE SWITCH]", "Switching to GOSSIP+SUSPICION mode")
				n.PutMembershipList(information)
			}
		} else if line == "LEAVE" {
			pid := os.Getpid()
			fmt.Printf("Current Process PID: %d\n", pid)
			syscall.Kill(pid, syscall.SIGTERM)
			n.terminate <- 1
			return
		} else if line == "LIST_SELF" {
			logger.Info("[SELF-ID]", n.NodeId)
		} else if line == "LIST_MEM" {
			n.PrintMembers(logger, filelogger, 1)
		} else {
			words := strings.Fields(line)
			if words[0] == "put" {
				// add handling of the file
				if len(words) < 3 || len(words) > 3 {
					continue
				}
				// n.Upload(words[1], words[2], config, logger, filelogger)
				n.Upload(words[1], words[2], config, logger)
			} else if words[0] == "get" {
				if len(words) < 3 || len(words) > 3 {
					continue
				}
				n.Download(words[2], words[1], config, logger)
			} else if words[0] == "delete" {
				if len(words) < 2 || len(words) > 2 {
					continue
				}
				n.FileDelete(words[1], config, logger)
			} else if words[0] == "ls" {
				if len(words) < 2 || len(words) > 2 {
					continue
				}
				n.ListLocations(words[1], config, logger, filelogger)
			} else if words[0] == "store" {
				n.ListStoredReplicas(logger)
			} else if words[0] == "multiread" {
				if len(words) < 4 || len(words) > 4 {
					continue
				}
				n.MultiDownload(words[2], words[1], words[3], config, logger)
			} else if words[0] == "maple" {
				// Risheek Change START
				if len(words) != 5 {
					logger.Info("[INPUT ERROR]", "Check the number of Arguments")
					continue
				}
				if n.CheckFile(words[1]) {
					namedKeyMap := make(map[string]string)
					numMaples, err := strconv.Atoi(words[2])
					if err != nil {
						fmt.Println(err)
						continue
					}
					logger.Info("[INPUT REQUIRED]", "Please enter the regExp for mapple:")
					scanner.Scan()
					targetColumn := scanner.Text()
					namedKeyMap["targetColumn"] = targetColumn
					logger.Info("[INPUT REQUIRED]", "Please enter the regExp for mapple:")
					scanner.Scan()
					regexp := scanner.Text()
					namedKeyMap["regExp"] = regexp
					logger.Info("[INPUT REQUIRED]", "Please enter column for grouping:")
					scanner.Scan()
					col := scanner.Text()
					namedKeyMap["col"] = col
					n.MappleCaller(words[1], numMaples, words[3], words[4], namedKeyMap, config, logger, filelogger)
				} else {
					logger.Info("[INPUT ERROR]", "Executable not present")
					continue
				}
				// Risheek Change END
			} else if words[0] == "juice" {
				if len(words) < 6 || len(words) > 6 {
					logger.Info("[INPUT ERROR]", "Check the number of Arguments")
					continue
				}
				if n.CheckFile(words[1]) && (words[5] == "0" || words[5] == "1") {
					// Risheek Change START
					numJuices, err := strconv.Atoi(words[2])
					if err != nil {
						fmt.Println(err)
						continue
					}
					n.JuiceCaller(words[1], numJuices, words[3], words[4], words[5], config, logger, filelogger)
					// Risheek Change END
				} else {
					logger.Info("[INPUT ERROR]", "Check the Input")
					continue
				}
				// Risheek change START
			} else if words[0] == "SELECT" {
				if words[1] == "ALL" {
					//SELECT ALL from Dataset where <column> REGEXP ""
					if words[6] == "REGEXP" {
						if n.CheckFile("filterMapple2") && n.CheckFile("filterJuice2") {
							namedKeyMap := make(map[string]string)
							logger.Info("[INPUT REQUIRED]", "Please mention the number of workers for mapple:")
							scanner.Scan()
							numWorkers := scanner.Text()
							numMaple, err := strconv.Atoi(numWorkers)
							if err != nil {
								fmt.Println("Something wrong with numMaples")
								continue
							}
							logger.Info("[INPUT REQUIRED]", "Please mention the number of workers for Juice:")
							scanner.Scan()
							numWorkersJ := scanner.Text()
							numJuice, err := strconv.Atoi(numWorkersJ)
							if err != nil {
								fmt.Println("Something wrong with numJuices")
								continue
							}
							logger.Info("[INPUT REQUIRED]", "Please mention the prefix for intermediary files:")
							scanner.Scan()
							prefix := scanner.Text()
							logger.Info("[INPUT REQUIRED]", "Please mention the final file name:")
							scanner.Scan()
							final := scanner.Text()
							var delete int
							for {
								logger.Info("[INPUT REQUIRED]", "Delete the temporary files?: (0 for \"NO\", 1 for \"YES\")")
								scanner.Scan()
								del, err := strconv.Atoi(scanner.Text())
								if err == nil {
									if del == 0 || del == 1 {
										delete = del
										break
									}
								}
							}
							fmt.Println(numWorkers, prefix, delete)
							namedKeyMap["targetColumn"] = words[5]
							namedKeyMap["regExp"] = words[7]
							namedKeyMap["final"] = final
							n.SQLCaller("FILTER", numMaple, numJuice, prefix, words[3], namedKeyMap, delete, config, logger, filelogger)

							// MappleInvoker("filterMapple2", words[3], 5, 10, words[5], words[7], prefix, 1, 1, "nil", config, logger, filelogger)
							// JuiceInvoker("filterJuice2", "test3_1_1_true.csv", final, config, logger, filelogger)
						} else {
							logger.Info("[404-FILE NOT FOUND]", "Please upload the mapple and juice executables to SDFS")
							continue
						}
					}
				} else if words[5] == "WHERE" {
					// Given two Datasets D1 and D2: SELECT ALL FROM D1, D2 WHERE <one specific field’s value in a line of D1 = one specific field’s value in a line of D2>,
					//call joinmap on dataset 1, call joinmap on dataset 2, call joinjuice

					//Remove , from dataset1

					if n.CheckFile("joinJuice") && n.CheckFile("joinMapple") {
						namedKeyMap := make(map[string]string)
						logger.Info("[INPUT REQUIRED]", "Please mention the number of workers for mapple:")
						scanner.Scan()
						numWorkers := scanner.Text()
						numMaple, err := strconv.Atoi(numWorkers)
						if err != nil {
							fmt.Println("Something wrong with numMaples")
							continue
						}
						logger.Info("[INPUT REQUIRED]", "Please mention the number of workers for Juice:")
						scanner.Scan()
						numWorkersJ := scanner.Text()
						numJuice, err := strconv.Atoi(numWorkersJ)
						if err != nil {
							fmt.Println("Something wrong with numJuices")
							continue
						}
						logger.Info("[INPUT REQUIRED]", "Please mention the prefix for intermediary files:")
						scanner.Scan()
						prefix := scanner.Text()
						logger.Info("[INPUT REQUIRED]", "Please mention the final file name:")
						scanner.Scan()
						final := scanner.Text()
						var delete int
						for {
							logger.Info("[INPUT REQUIRED]", "Delete the temporary files?: (0 for \"NO\", 1 for \"YES\")")
							scanner.Scan()
							del, err := strconv.Atoi(scanner.Text())
							if err == nil {
								if del == 0 || del == 1 {
									delete = del
									break
								}
							}
						}
						fmt.Println(numWorkers, prefix, delete, final)
						namedKeyMap["targetColumn"] = words[6]
						namedKeyMap["targetColumn2"] = words[7]
						namedKeyMap["final"] = final
						namedKeyMap["Data2"] = words[4]
						n.SQLCaller("JOIN", numMaple, numJuice, prefix, words[3], namedKeyMap, delete, config, logger, filelogger)
					} else {
						logger.Info("[404-FILE NOT FOUND]", "Please upload the mapple and juice executables to SDFS")
						continue
					}
				} else {
					continue
				}
				// Risheek change END
			} else {
				continue
			}
		}
	}

}
