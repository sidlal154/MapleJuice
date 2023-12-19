package client

import (
	"context"
	"google.golang.org/grpc"
	"mp4-gossip/logger"
	pb "mp4-gossip/protos/grpcServer"
)

type Client struct {
	logger *logger.CustomLogger
}

func NewClient(logger *logger.CustomLogger) *Client {
	return &Client{
		logger: logger,
	}
}

func (c *Client) ClientSendRead(address string, req *pb.SendReadRequest) (*pb.SendReadResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientSendRead on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.SendRead(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, SendRead Failed", err)
		return nil, err
	}
	return resp, nil
}

func (c *Client) ClientGetReadFile(address string, req *pb.GetReadFileRequest) (*pb.GetByteArrayResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientGetReadFile on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)

	serverStream, err := client.GetReadFile(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error creating client stream", err)
		return nil, err
	}

	for {
		response, err := serverStream.Recv()
		if err != nil {
			c.logger.Error("[CLIENT]", "Error From server, ClientGetReadFile Failed", err)
			return nil, err
		}
		return response, err
	}

}

func (c *Client) ClientSendWrite(address string, req *pb.SendWriteRequest) (*pb.SendWriteResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientSendWrite on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)

	clientStream, err := client.SendWrite(context.Background())
	if err != nil {
		c.logger.Error("[CLIENT]", "Error creating client stream", err)
		return nil, err
	}
	err = clientStream.Send(req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error sending request", err)
		return nil, err
	}

	resp, err := clientStream.CloseAndRecv()
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, ClientSendWrite Failed", err)
		return nil, err
	}

	return resp, nil
}

func (c *Client) ClientRemoteRead(address string, req *pb.RemoteReadRequest) (*pb.RemoteReadResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientRemoteRead on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.RemoteRead(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, RemoteRead Failed", err)
		return nil, err
	}
	return resp, nil

}

func (c *Client) ClientDeleteFile(address string, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientDeleteFile on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.DeleteFile(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, DeleteFile Failed", err)
		return nil, err
	}
	return resp, nil

}

func (c *Client) ClientEnqueue(address string, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientEnqueue on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.Enqueue(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, ClientEnqueue Failed", err)
		return nil, err
	}
	return resp, nil

}

func (c *Client) ClientDequeue(address string, req *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientDequeue on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.Dequeue(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, ClientDequeue Failed", err)
		return nil, err
	}
	return resp, nil

}

func (c *Client) ClientTopPeek(address string, req *pb.TopPeekRequest) (*pb.TopPeekResponse, error) {
	// Set up a connection to the gRPC server
	c.logger.Info("[CLIENT]", "Initialed a ClientTopPeek on:"+address)
	conn, err := grpc.Dial(address+":50051", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(3*1024*1024*1024)))
	if err != nil {
		c.logger.Error("[CLIENT]", "Failed to connect", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewGrpcServerClient(conn)
	resp, err := client.TopPeek(context.Background(), req)
	if err != nil {
		c.logger.Error("[CLIENT]", "Error From server, ClientTopPeek Failed", err)
		return nil, err
	}
	return resp, nil

}
