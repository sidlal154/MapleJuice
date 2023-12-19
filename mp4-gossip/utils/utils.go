package utils

import (
	"bytes"
	"encoding/gob"
	"math"
	"mp4-gossip/logger"
	"net"
)

type GossipMember struct {
	NodeId     string
	Heartbeat  int64
	Localtime  int64
	Sus        bool
	Incarno    int64
	FileList   []string
	LeaderList []string
}

type MembershipList struct {
	Member      []*GossipMember
	MessageType int64
	IsSus       bool
	Version     int
}

type TaskTracker struct {
	TaskName  string
	TaskRange string
	InputFile string
	Prefix    string
	Status    string
}

func Read(conn *net.UDPConn, logger *logger.CustomLogger) (*MembershipList, *net.UDPAddr, error) {

	p := make([]byte, 4096)
	_, remoteaddr, err := conn.ReadFromUDP(p)
	if err != nil {
		logger.Error("[UTILS]", "Error in reading from UDP: ", err)
		return nil, nil, err
	}
	//logger.Info("[UTILS]", "p read: "+string(p))
	/*
		members := &pb.MembershipList{}

		err = proto.Unmarshal(p, members)

		if err != nil {
			logger.Error("[UTILS]","Error in unmarshalling: ", err)
			return nil,nil,err
		}
		return members, remoteaddr, nil
	*/

	members := &MembershipList{}

	k := bytes.NewBuffer(p)
	dec := gob.NewDecoder(k)
	err = dec.Decode(members)
	if err != nil {
		logger.Error("[UTILS]", "Error in decoding: ", err)
		return nil, nil, err
	}
	return members, remoteaddr, err
}

func ReadClient(conn *net.UDPConn, logger *logger.CustomLogger) (*MembershipList, error) {
	p := make([]byte, 4096)
	_, err := conn.Read(p)
	if err != nil {
		logger.Error("[UTILS]", "Error in reading from UDP: ", err)
		return nil, err
	}
	/*
		members := &pb.MembershipList{}
		err = proto.Unmarshal(p, members)
		if err != nil {
			logger.Error("[UTILS]","Error in unmarshalling: ", err)
			return nil,err
		}
		return members, err*/

	members := &MembershipList{}

	k := bytes.NewBuffer(p)
	dec := gob.NewDecoder(k)
	err = dec.Decode(members)
	if err != nil {
		logger.Error("[UTILS]", "Error in decoding: ", err)
		return nil, err
	}
	return members, nil
}

func Write(conn *net.UDPConn, addr *net.UDPAddr, payload *MembershipList, logger *logger.CustomLogger) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(payload)
	if err != nil {
		logger.Error("[UTILS]", "Error in encoding: ", err)
		return err
	}
	/*writeBytes, err := proto.Marshal(payload)
	if err != nil {
		logger.Error("[UTILS]","Error in marshalling: ", err)
		return err
	}*/
	_, err = conn.WriteToUDP(buf.Bytes(), addr)
	if err != nil {
		logger.Error("[UTILS]", "Error in writing to UDP: ", err)
		return err
	}
	/*
		enc := gob.NewEncoder(conn)
		err := enc.Encode(payload)
		return err*/
	return nil
}

func WriteClient(conn *net.UDPConn, payload *MembershipList, logger *logger.CustomLogger) error {
	/*
		fmt.Println("Members: ",payload)
		writeBytes, err := proto.Marshal(payload)
		members := &pb.MembershipList{}
		err = proto.Unmarshal(writeBytes, members)
		fmt.Println("Members: ",members)
		_, err = conn.Write(writeBytes)
		fmt.Println("bytes written is: "+string(writeBytes))
		return err*/
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(payload)
	if err != nil {
		logger.Error("[UTILS]", "Error in encoding: ", err)
		return err
	}
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		logger.Error("[UTILS]", "Error in writing: ", err)
		return err
	}
	return err
}

func CheckMember(NodeId string, list []*GossipMember) *GossipMember {
	for _, member := range list {
		if member.NodeId == NodeId {
			return member
		}
	}
	return nil
}

func CheckInArray(str string, arr []string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func DeleteInArray(str string, arr []string) []string {
	var res []string
	for _, a := range arr {
		if a != str {
			res = append(res, a)
		}
	}
	return res
}

func CeilDivide(dividend int, divisor int) int {
	quotient := float64(dividend) / float64(divisor)
	result := int(math.Ceil(quotient))
	return result
}
