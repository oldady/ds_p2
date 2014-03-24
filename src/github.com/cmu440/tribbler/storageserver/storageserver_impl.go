package storageserver

import (
	"container/list"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var _ = fmt.Println

type storageServer struct {
	// server infos
	numNodes  int
	port      int
	nodeID    uint32
	isMaster  bool
	servers   map[uint32]storagerpc.Node
	isReady   bool
	infoRWL   sync.RWMutex
	readyChan chan struct{}

	store    map[string]interface{}
	leases   map[string]*list.List
	storeRWL sync.RWMutex
}

type leaseRecord struct {
	expirationTime time.Time
	hostport       string
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := &storageServer{
		numNodes:  numNodes,
		port:      port,
		nodeID:    nodeID,
		readyChan: make(chan struct{}),
		servers:   make(map[uint32]storagerpc.Node),
		store:     make(map[string]interface{}),
		leases:    make(map[string]*list.List),
	}
	if masterServerHostPort == "" {
		ss.isMaster = true
	}

	// register RPCs
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(l, nil)

	if ss.isMaster {
		if ss.numNodes > 1 {
			<-ss.readyChan // wait for slaves
		}
	} else {
		err := ss.joinCluster(masterServerHostPort)
		if err != nil {
			return nil, err
		}
	}

	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.isMaster {
		return errors.New("server is not a master")
	}

	ss.infoRWL.Lock()
	defer ss.infoRWL.Unlock()

	// add to cluster
	if _, exisit := ss.servers[args.ServerInfo.NodeID]; !exisit {
		ss.servers[args.ServerInfo.NodeID] = args.ServerInfo
	}

	reply.Status = storagerpc.NotReady
	if len(ss.servers) == ss.numNodes {
		ss.isReady = true
		close(ss.readyChan)

		// make reply
		reply.Status = storagerpc.OK
		for k := range ss.servers {
			reply.Servers = append(reply.Servers, ss.servers[k])
		}
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.infoRWL.RLock()
	defer ss.infoRWL.RUnlock()

	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}
	reply.Status = storagerpc.OK
	for k := range ss.servers {
		reply.Servers = append(reply.Servers, ss.servers[k])
	}

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	generalReply, value := ss.generalGet(args)

	reply.Status, reply.Lease = generalReply.Status, generalReply.Lease
	reply.Value = value.(string)

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	generalReply, value := ss.generalGet(args)

	reply.Status, reply.Lease = generalReply.Status, generalReply.Lease
	reply.Value = value.([]string)

	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	status, ok := ss.assertKeyAndServer(args.Key)
	if !ok {
		reply.Status = status
		return nil
	}

	ss.storeRWL.Lock()
	defer ss.storeRWL.Unlock()

	_, exisit := ss.store[args.Key]
	if exisit && isUserKey(args.Key) {
		reply.Status = storagerpc.ItemExists
		return nil
	}

	if err := ss.revokeLease(args.Key); err != nil {
		panic("")
	}
	ss.store[args.Key] = args.Value
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	status, ok := ss.assertKeyAndServer(args.Key)
	if !ok {
		reply.Status = status
		return nil
	}

	ss.storeRWL.Lock()
	defer ss.storeRWL.Unlock()

	_, exisit := ss.store[args.Key]
	if !exisit {
		// create a new list for the key
		ss.store[args.Key] = list.New()
		ss.store[args.Key].(*list.List).PushBack(args.Value)
		reply.Status = storagerpc.OK
		return nil
	}

	// test item exisitence
	l := ss.store[args.Key].(*list.List)
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == args.Value {
			reply.Status = storagerpc.ItemExists
			return nil
		}
	}

	if err := ss.revokeLease(args.Key); err != nil {
		panic("")
	}
	l.PushBack(args.Value)
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	status, ok := ss.assertKeyAndServer(args.Key)
	if !ok {
		reply.Status = status
		return nil
	}

	ss.storeRWL.Lock()
	defer ss.storeRWL.Unlock()

	if _, exisit := ss.store[args.Key]; !exisit {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	// test item exisitence
	l := ss.store[args.Key].(*list.List)
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == args.Value {
			if err := ss.revokeLease(args.Key); err != nil {
				panic("")
			}
			l.Remove(e)
			reply.Status = storagerpc.OK
			return nil
		}
	}

	// item not found
	reply.Status = storagerpc.ItemNotFound
	return nil
}

// assert: 1, if the server is ready
//         2, if the key is in the range of the server
// return true if ok, false otherwise
func (ss *storageServer) assertKeyAndServer(key string) (storagerpc.Status, bool) {
	ss.infoRWL.RLock()
	defer ss.infoRWL.RUnlock()

	if !ss.isReady {
		return storagerpc.NotReady, false
	}
	if !ss.inRange(key) {
		return storagerpc.WrongServer, false
	}
	return storagerpc.OK, true
}

// Return true if the key is in this server's range,
// else, return false
func (ss *storageServer) inRange(key string) bool {
	// get username part
	index := strings.Index(key, ":")
	if index < 0 {
		panic("")
	}
	hash := libstore.StoreHash(key[0:index])

	var successor uint32
	distance := ^uint32(0)
	for id := range ss.servers {
		tmpDistance := uint32(id - hash) // [*]auto overflow, maybe -1
		if tmpDistance < distance {
			tmpDistance = distance
			successor = id
		}
	}
	if successor != ss.nodeID {
		return false
	}
	return true
}

func isUserKey(key string) bool {
	index := strings.Index(key, ":")
	return key[index:index+4] == "user"
}

// Get a key-value pair
func (ss *storageServer) generalGet(args *storagerpc.GetArgs) (*storagerpc.GetReply, interface{}) {
	status, ok := ss.assertKeyAndServer(args.Key)
	if !ok {
		return &storagerpc.GetReply{Status: status}, nil
	}

	ss.storeRWL.RLock()
	defer ss.storeRWL.RUnlock()

	value, exisit := ss.store[args.Key]
	if !exisit {
		return &storagerpc.GetReply{Status: storagerpc.KeyNotFound}, nil
	}

	if args.WantLease {
		leaseList := ss.leases[args.Key]
		if leaseList == nil {
			ss.leases[args.Key] = list.New()
			leaseList = ss.leases[args.Key]
		}

		// append a lease record
		leaseSeconds := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
		leaseList.PushBack(&leaseRecord{
			expirationTime: time.Now().Add(time.Second * time.Duration(leaseSeconds)),
			hostport:       args.HostPort,
		})
	}

	// send back reply
	reply := &storagerpc.GetReply{
		Status: storagerpc.OK,
		Lease: storagerpc.Lease{
			Granted:      true,
			ValidSeconds: storagerpc.LeaseSeconds,
		},
	}

	return reply, value
}

// Revoke leases for the givin key from all lease holders.
// This func assume that it has already get a WLock
func (ss *storageServer) revokeLease(key string) error {
	leaseList, exisit := ss.leases[key]
	if !exisit {
		return nil
	}

	for e := leaseList.Front(); e != nil; e = e.Next() {
		lr := e.Value.(*leaseRecord)
		if lr.expirationTime.After(time.Now()) {
			// not expired, need to revoke
			client, err := rpc.DialHTTP("tcp", lr.hostport)
			if err != nil {
				return err
			}

			args := &storagerpc.RevokeLeaseArgs{Key: key}
			var reply storagerpc.RevokeLeaseReply

			err = client.Call("LeaseCallbacks.RevokeLease", args, &reply)
			if err != nil {
				return err
			}
			if reply.Status != storagerpc.OK {
				return errors.New("RevokeLease returns not OK status")
			}
		}
		leaseList.Remove(e)
	}
	delete(ss.leases, key)

	return nil
}

func (ss *storageServer) joinCluster(masterServerHostPort string) error {
	for {
		master, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return err
		}

		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				HostPort: net.JoinHostPort("localhost", strconv.Itoa(ss.port)),
				NodeID:   ss.nodeID,
			},
		}
		var reply storagerpc.RegisterReply

		err = master.Call("StorageServer.RegisterServer", args, &reply)
		if err != nil {
			return err
		}

		if reply.Status == storagerpc.OK {
			ss.infoRWL.Lock()
			for _, node := range reply.Servers {
				ss.servers[node.NodeID] = node
			}
			ss.isReady = true
			ss.infoRWL.Unlock()
			return nil
		}

		time.Sleep(1 * time.Second)
	}
}
