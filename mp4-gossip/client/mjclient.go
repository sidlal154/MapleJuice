package client

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	pb "mp4-gossip/protos/grpcServer"
	"mp4-gossip/utils"
)

// Vanilla Maple Input handler
func (c *Client) ClientNotifyMaple(address string, req *pb.NotifyMapleRequest) (*pb.NotifyMapleResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[MJ-CLIENT]", "Initialed a ClientNotifyMaple on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.NotifyMaple(context.Background(), req)
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error From server, ClientNotifyMaple Failed", err)
		return nil, err
	}
	return resp, nil
}

// Vanilla Juice Input handler
func (c *Client) ClientNotifyJuice(address string, req *pb.NotifyJuiceRequest) (*pb.NotifyJuiceResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[MJ-CLIENT]", "Initialed a ClientNotifyJuice on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.NotifyJuice(context.Background(), req)
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error From server, ClientNotifyJuice Failed", err)
		return nil, err
	}
	return resp, nil
}

// SQL input Handler
func (c *Client) ClientNotifySQL(address string, req *pb.NotifySQLRequest) (*pb.NotifySQLResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[MJ-CLIENT]", "Initialed a ClientNotifySQL on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.NotifySQL(context.Background(), req)
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error From server, ClientNotifySQL Failed", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) ClientAllotMaple(trackerTasks []*utils.TaskTracker, address string, req *pb.AllotMapleRequest) ([]string, []*utils.TaskTracker, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[MJ-CLIENT]", "Initialed a ClientAllotMaple on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Failed to connect", err)
		return nil, nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)

	serverStream, err := client.AllotMaple(context.Background(), req)
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error creating client stream", err)
		return nil, nil, err
	}

	var tasksStatus []string
	for {
		response, err := serverStream.Recv()
		if err != nil {
			// Check for the end of the stream
			if err == context.Canceled {
				break
			}
			c.logger.Info("[CLIENT]", "Error From server, ClientAllotMaple Failed")
			break
		}
		tasksStatus = append(tasksStatus, response.Status)

		for _, task := range trackerTasks {
			if response.Status == task.TaskRange {
				task.Status = "done"
			}
		}

	}

	return tasksStatus, trackerTasks, nil

}

func (c *Client) ClientAllotJuice(trackerTasks []*utils.TaskTracker, address string, req *pb.AllotJuiceRequest) ([]string, []*utils.TaskTracker, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[MJ-CLIENT]", "Initialed a ClientAllotJuice on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Failed to connect", err)
		return nil, nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)

	serverStream, err := client.AllotJuice(context.Background(), req)
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error creating client stream", err)
		return nil, nil, err
	}

	var tasksStatus []string
	for {
		response, err := serverStream.Recv()
		if err != nil {
			// Check for the end of the stream
			if errors.Is(err, context.Canceled) {
				break
			}
			c.logger.Info("[MJ-CLIENT]", "Error From server, ClientAllotJuice Failed")
			break
			//TODO: MP4: here the task line by line status arrives, use this and a global list with the leader to track the worker status
		}
		tasksStatus = append(tasksStatus, response.Status)

		for _, task := range trackerTasks {
			if response.Status == task.TaskRange {
				task.Status = "done"
			}
		}
	}

	return tasksStatus, trackerTasks, nil

}

func (c *Client) ClientSendFile(address string, req *pb.SendWriteRequest) (*pb.SendWriteResponse, error) {

	c.logger.Info("[MJ-CLIENT]", "Initialed a ClientSendWrite on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)

	clientStream, err := client.SendFile(context.Background())
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error creating client stream", err)
		return nil, err
	}
	err = clientStream.Send(req)
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error sending request", err)
		return nil, err
	}

	resp, err := clientStream.CloseAndRecv()
	if err != nil {
		c.logger.Error("[MJ-CLIENT]", "Error From server, ClientSendWrite Failed", err)
		return nil, err
	}

	return resp, nil

}
