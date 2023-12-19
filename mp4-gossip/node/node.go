package node

import (
	"golang.org/x/exp/slices"
	"mp4-gossip/client"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/queue"
	"mp4-gossip/utils"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LockedMList struct {
	mu             sync.RWMutex
	MembershipList *utils.MembershipList
}

type SafeMap struct {
	mu   sync.Mutex
	data map[string]*Queue
}

type SafeTracker struct {
	mu      sync.Mutex
	tracker map[string][]*utils.TaskTracker
}

func (s *SafeMap) Set(key string, value *Queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *SafeMap) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *SafeMap) Get(key string) *Queue {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key]
}

type Node struct {
	NodeId         string
	MembershipList LockedMList
	Listener       *net.UDPConn
	MyIncarnation  int
	Version        int
	reqTime        chan int
	currTime       chan int64
	terminate      chan int
	counterList    []string
	// leaderList     map[string]*RWMaintain
	// leaderChannel  map[string]chan int
	LeaderList     *SafeMap
	addCh          chan string
	deleteCh       chan string
	GrpcClient     *client.Client
	replicaTracker map[string][]string
	MapleTracker   SafeTracker
	JuiceTracker   SafeTracker
	SQLQueue       queue.MJQueue
	failStop bool
}

func (n *Node) GetMembershipList() *utils.MembershipList {
	n.MembershipList.mu.Lock()
	defer n.MembershipList.mu.Unlock()
	return n.MembershipList.MembershipList
}

func (n *Node) PutMembershipList(m *utils.MembershipList) {
	n.MembershipList.mu.Lock()
	defer n.MembershipList.mu.Unlock()
	n.MembershipList.MembershipList = m
}

func (n *Node) GetMapleTracker() map[string][]*utils.TaskTracker {
	n.MapleTracker.mu.Lock()
	defer n.MapleTracker.mu.Unlock()
	return n.MapleTracker.tracker
}

func (n *Node) PutMapleTracker(m map[string][]*utils.TaskTracker) {
	n.MapleTracker.mu.Lock()
	defer n.MapleTracker.mu.Unlock()
	n.MapleTracker.tracker = m
}

func (n *Node) GetJuiceTracker() map[string][]*utils.TaskTracker {
	n.JuiceTracker.mu.Lock()
	defer n.JuiceTracker.mu.Unlock()
	return n.JuiceTracker.tracker
}

func (n *Node) PutJuiceTracker(m map[string][]*utils.TaskTracker) {
	n.JuiceTracker.mu.Lock()
	defer n.JuiceTracker.mu.Unlock()
	n.JuiceTracker.tracker = m
}

func (n *Node) GetMapleTrackerCopy() map[string][]utils.TaskTracker {
	n.MapleTracker.mu.Lock()
	defer n.MapleTracker.mu.Unlock()
	orignal := n.MapleTracker.tracker
	copy := make(map[string][]utils.TaskTracker)
	for key, value := range orignal {
		var newValue []utils.TaskTracker
		for _, v := range value {
			newValue = append(newValue, *v)
		}
		copy[key] = newValue
	}
	return copy
}

func (n *Node) GetJuiceTrackerCopy() map[string][]utils.TaskTracker {
	n.JuiceTracker.mu.Lock()
	defer n.JuiceTracker.mu.Unlock()
	orignal := n.JuiceTracker.tracker
	copy := make(map[string][]utils.TaskTracker)
	for key, value := range orignal {
		var newValue []utils.TaskTracker
		for _, v := range value {
			newValue = append(newValue, *v)
		}
		copy[key] = newValue
	}
	return copy
}

func StartNewNode(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger, mode int) *Node {
	s, err := net.ResolveUDPAddr("udp", ":"+config.Port)
	if err != nil {
		logger.Error("[NODE]", "couldn't resolve UDP addrs", err)
		filelogger.Error("[NODE]", "couldn't resolve UDP addrs", err)
		return nil
	}
	listner, err := net.ListenUDP("udp", s)
	if err != nil {
		logger.Error("[NODE]", "Error while starting the node", err)
		filelogger.Error("[NODE]", "Error while starting the node", err)
		return nil
	}
	//launchtime := strconv.Itoa(time.Now().UnixMilli())
	//TODO: delete contents of config.Sdfsdir
	utils.EmptyFromDirectory(config.SdfsDir)

	launchtime := time.Now().Unix()
	var stringArray []string
	mem := &utils.GossipMember{
		NodeId:    config.MachineName + "#" + strconv.FormatInt(launchtime, 10),
		Heartbeat: 1,
		Localtime: 1,
		Sus:       false,
		Incarno:   0,
		FileList:  stringArray,
	}
	members := &utils.MembershipList{}
	members.Member = append(members.Member, mem)
	members.MessageType = int64(mode)
	members.IsSus = false
	logger.Info("[NODE]", "New node started at "+config.MachineName)
	//filelogger.Info("[NODE]", "New node started at "+config.MachineName)

	lockedMembers := LockedMList{MembershipList: members}
	c := client.NewClient(logger)
	return &Node{
		NodeId:         config.MachineName + "#" + strconv.FormatInt(launchtime, 10),
		MembershipList: lockedMembers,
		Listener:       listner,
		MyIncarnation:  0,
		Version:        0,
		reqTime:        make(chan int),
		currTime:       make(chan int64),
		terminate:      make(chan int),
		counterList:    make([]string, 0),
		LeaderList:     &SafeMap{mu: sync.Mutex{}, data: make(map[string]*Queue)},
		addCh:          make(chan string),
		deleteCh:       make(chan string),
		GrpcClient:     c,
		replicaTracker: make(map[string][]string),
		MapleTracker:   SafeTracker{mu: sync.Mutex{}, tracker: make(map[string][]*utils.TaskTracker)},
		JuiceTracker:   SafeTracker{mu: sync.Mutex{}, tracker: make(map[string][]*utils.TaskTracker)},
		SQLQueue:       queue.MJQueue{},
		// leaderChannel:  make(map[string]chan int),
		failStop: false,
	}
}

func UDPIntroducerSendHandler(payload *utils.MembershipList, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) (*utils.MembershipList, error) {
	//filelogger.Info("[NODE]", "Inside UDPIntroducerSendHandler")
	ServerAddr, err := net.ResolveUDPAddr("udp", config.Introducer+":"+config.Port)
	if err != nil {
		logger.Error("[NODE]", "Error in resolving UDP addr: ", err)
		filelogger.Error("[NODE]", "Error in resolving UDP addr: ", err)
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, ServerAddr)

	if err != nil {
		logger.Error("[NODE]", "Error connecting to server:", err)
		filelogger.Error("[NODE]", "Error in resolving UDP addr: ", err)
		return nil, err
	}
	defer conn.Close()
	err = utils.WriteClient(conn, payload, logger)
	if err != nil {
		logger.Error("[NODE]", "Error writing to server:", err)
		filelogger.Error("[NODE]", "Error writing to server:", err)
		return nil, err
	}

	resp, err := utils.ReadClient(conn, logger)
	if err != nil {
		logger.Error("[NODE]", "Error reading from server", err)
		filelogger.Error("[NODE]", "Error writing to server:", err)
		return nil, err
	}

	return resp, nil
}

func (n *Node) Introduce(config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger, wg *sync.WaitGroup) {
	defer wg.Done()
	//filelogger.Info("[NODE]", "Sending a Hi to Introducer")
	req := n.GetMembershipList()
	reqNew := &utils.MembershipList{
		Member:      req.Member,
		MessageType: 2,
		IsSus:       req.IsSus,
		Version:     req.Version,
	}
	//customPrint(reqNew, filelogger, "REQUEST")
	resp, err := UDPIntroducerSendHandler(reqNew, config, logger, filelogger)
	if err != nil {
		logger.Error("[NODE]", "Error while sending Introduction: ", err)
		filelogger.Error("[NODE]", "Error while sending Introduction: ", err)
		return
	}

	memberList := resp.Member
	for _, memberEntry := range memberList {
		memberEntry.Localtime = 2
	}

	n.PutMembershipList(resp)

	//filelogger.Info("[NODE]", "Membership List updated")
	//n.PrintMembers(logger, filelogger, 2)

}

func (n *Node) HandleIntroducer(req *utils.MembershipList, addr *net.UDPAddr, config *config.Config, logger *logger.CustomLogger, filelogger *filelogger.FileLogger) {
	sender := req.Member[0]
	//filelogger.Info("[NODE]", "New introducer message received from: "+sender.NodeId)

	myMem := n.GetMembershipList()
	myMemList := myMem.Member
	var subList []*utils.GossipMember
	if len(myMemList) > 3 {
		subList = myMemList[len(myMemList)-3:]
	} else {
		subList = myMemList
	}
	subList = append(subList, sender)
	members := &utils.MembershipList{}
	members.Member = subList
	members.MessageType = myMem.MessageType
	members.IsSus = false
	err := utils.Write(n.Listener, addr, members, logger)
	if err != nil {
		logger.Error("[NODE]", "Error while writing Introducer response", err)
		filelogger.Error("[NODE]", "Error while writing Introducer response", err)
		return
	}
	myMemList = append(myMemList, sender)

	n.PutMembershipList(myMem)
	//filelogger.Info("[NEW-NODE-ADDITION]", "Membership List updated via Introducer")
	//n.PrintMembers(logger, filelogger, 2)
}

func (n *Node) CheckLeader(file string) string {
	information := n.GetMembershipList()
	memList := information.Member
	for _, member := range memList {
		if slices.Contains(member.LeaderList, file) {
			return strings.Split(member.NodeId, "#")[0]
		}
	}
	return "NONE"
}

func (n *Node) GetMapleTrackerKey() []string {
   n.MapleTracker.mu.Lock()
   defer n.MapleTracker.mu.Unlock()
   var memberAlloted []string
   for memberId, _ := range n.MapleTracker.tracker {
      memberAlloted = append(memberAlloted, memberId)
   }
   return memberAlloted
}

func (n *Node) GetJuiceTrackerKey() []string {
   n.JuiceTracker.mu.Lock()
   defer n.JuiceTracker.mu.Unlock()
   var memberAlloted []string
   for memberId, _ := range n.JuiceTracker.tracker {
      memberAlloted = append(memberAlloted, memberId)
   }
   return memberAlloted
}

