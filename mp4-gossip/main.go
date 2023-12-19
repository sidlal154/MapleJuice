package main

import (
	"flag"
	"mp4-gossip/config"
	"mp4-gossip/filelogger"
	"mp4-gossip/logger"
	"mp4-gossip/server"
)

func main() {
	//Flag to set log level and testing
	help := flag.Bool("h", false, "help")

	logLevel := "info"
	mode := 1
	flag.IntVar(&mode, "mode", 1, "sets the mode to use: 1 is the gossip mode, 3 is the gossip+Suspicion mode")
	flag.StringVar(&logLevel, "loglev", "info", "Set log level: can be debug, info, error. Any other value returns info, set to info by default")
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	//declaring logger and configs
	Logger := logger.NewLogger(logLevel)
	config := config.NewConfig(Logger)
	FileLogger := filelogger.NewFileLogger(config)
	Logger.Info("[MAIN]", "Welcome to CS425 mp4, Using log level: "+logLevel)

	/*Node := node.StartNewNode(config, Logger, FileLogger, mode)

	var wg sync.WaitGroup

	FileLogger.Info("[MAIN]", "Membership List first element: "+Node.GetMembershipList().Member[0].NodeId)
	if config.MachineName != config.Introducer {
		wg.Add(1)
		go Node.Introduce(config, Logger, FileLogger, &wg)
		wg.Wait()
	}

	go Node.ClientImpl(Logger, config, FileLogger)

	// Start of mp4
	go Node.ServerImpl(Logger, config, FileLogger)
	Logger.Info("[MAIN]", "Started Gossip Protocol")*/
	Server := server.NewServer(config, Logger, FileLogger, mode)
	go Server.LaunchServer()
	Server.LaunchNode(config, Logger, FileLogger)
	for {
	}
}
