package config

import (
	"mp4-gossip/logger"
	"os"

	"gopkg.in/yaml.v3"
)

/*
Port: port on which server process runs
AddressPath: filename and path of the membership list file
TimeOut: time the client thread waits for the server thread to send a response
LogPath: path of the log files
*/
type Config struct {
	Port        string `yaml:"port"`
	MachineName string `yaml:"machineName"`
	Introducer  string `yaml:"introducerId"`
	Tgossip     int    `yaml:"tgossip"`
	Tfail       int    `yaml:"tfail"`
	Tcleanup    int    `yaml:"tcleanup"`
	Bb          int    `yaml:"b"`
	DropRate    int    `yaml:"dropRate"`
	Replica     int    `yaml:"replica"`
	NetworkSize int    `yaml:"networkSize"` //to determine the hash
	Maxreaders  int    `yaml:"maxReaders"`
	MaxWriters  int    `yaml:"maxWriters"`
	SdfsDir     string `yaml:"sdfsdir"`
	LocalDir    string `yaml:"localdir"`
}

/*
unmarshalls the config.yaml file to initialize the given variables
returns the Config struct
*/
func NewConfig(logger *logger.CustomLogger) *Config {

	yamlBytes, err := os.ReadFile("config/config.yaml")
	if err != nil {
		logger.Error("[CONFIG]", "Some error while reading config file", err)
		return nil
	}

	config := &Config{}
	err = yaml.Unmarshal(yamlBytes, &config)
	if err != nil {
		logger.Error("[CONFIG]", "Some error while reading config file", err)
		return nil
	}

	return config
}
