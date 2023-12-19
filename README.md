# MapleJuice

The MapleJuice focuses on making a distributed task scheduler over a distributed file system using a gossip based membership protocol. MapleJuice has a leader-worker architecture, and closely resembles MapReduce, and can handle failure of worker nodes. It also has an SQL parser to run SQL-like queries.

## Overview

MapleJuice has been built using Golang. It is built on top of a Gossip based membership protocol, which supports dynamic joins and leaves from the network, along with fault tolerance, and an SDFS. Additionally, we use gRPC (port 50051) to handle all the networking requests apart from those used by gossip.

## Usage

- Use the config file at `config/config.yaml` to change the configurations of the network as follows:
```
Constants to be defined in config.yaml file
port : The default port running the server (currently 4040)
machineName: The name of the current machine (For eg. fa23-cs425-XXXX.cs.illinois.edu)
introducerId: The name of the introducer machine. We assume that whena network starts, it is fixed and it doesn't crash.
tgossip: Denotes the time period for gossiping in ms
tfail: Denotes T_fail in ms, the time by which if a node doesn't receive a heartbeat, it marks the node as failed (or suspicious)
tcleanup: Denotes T_cleanup in ms, the time a node waits to purge a failed entry in its membershipList
b: The number of nodes selected randomly to push gossip messages to
dropRate: The manual network latency we introduce. This denotes a percentage probability in my connection dropping
sdfsdir: The directory where sdfs storage is being managed
localdir: the local directory on the machine from where puts will happen, and into which all gets will get dumped
```
- The program invokes the client and server implementations on the machines it is run.
- There is a seperate thread on each VM which handles the gRPC Server
- The Client implementation first gets the response from the introducer, and then the server implementation is launched. Both run concurrently. The gRPC running the sdfs + maplejuice functions is also launched concurrently
- GRPC runs on port 50051

## Installation and running

- Install Golang version 1.19
- Clone the repository
- Build the project

```
cd mp4-gossip
go build
```

- Run the program in mp4-gossip directory. loglev can be info, debug or error. by default, loglev is info, and mode is 1

```
./mp4-gossip -loglev=<LogLevel>
```

- The above program will launch a Peer node. We have implemented the following command lines that give Membership list related and SDFS related functionality (same as MP-2,MP-3). In addition to this, we have also implemented the command line arguments for MapleJuice (MP-4)
```
LIST_MEM: list the current membership list
LIST_SELF: list self’s id
LEAVE: voluntarily leave the group
ENABLE_SUSPICION: enable the suspicion mode
DISABLE_SUSPICION: disbale the suspicion mode
put localfilename sdfsfilename: fetches from localfilename in config.localdir directory and uploads as sdfsfilename into sdfs (will act as an update as well if sdfsfilename exists)
get sdfsfilename localfilename: gets from sdfsfilename and stores in in localfilename under config.localdir directory
delete sdfsfilename: delete the file from sdfs
ls sdfsfilename: displays all VMs where sdfsfilename is replicated
store: List all the replicas which are currently stored in this VM
multiread sdfsfilename localfilename Vmi,....,Vmj : This performs remote gets on VMi,,,VMj. VM1 must be in the same format as the machine name i.e. fa23XXX...
maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>: This runs the maple_exe task on num_maple times, taking input from the sdfs_src_directory
juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}: This runs the juice_exe task on num_juices times, dumping the final output on sdfs_dest_filename in sdfs

```
In addition to this, you can also run vanilla SQL commands in the following format (as mentioned in the mp spec):
1. SELECT ALL FROM Dataset WHERE regex-condition
2. SELECT ALL FROM D1, D2 WHERE <one specific field’s value in a line of D1 = one specific field’s value in a line of D2>


Note: Here we have assumed that introducer is launched first before all nodes are launched, and the introducer is the MapleJuice Leader.

## Acknowwledgement

This Repo is completely designed, developed and maintained by Siddharth Lal and Risheek Rakshit SK as a part of a Programming Assignment for the course 'Distributed Systems' by Prof. Indranil Gupta


