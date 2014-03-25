package storageserver

import (
	"container/list"
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var leaseSeconds = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

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

	store      map[string]interface{}
	leases     map[string]*list.List
	storeRWL   sync.RWMutex
	inRevoking map[string]bool
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
		numNodes:   numNodes,
		port:       port,
		nodeID:     nodeID,
		readyChan:  make(chan struct{}),
		servers:    make(map[uint32]storagerpc.Node),
		store:      make(map[string]interface{}),
		leases:     make(map[string]*list.List),
		inRevoking: make(map[string]bool),
	}

	if masterServerHostPort == "" {
		ss.isMaster = true
	}
	ss.servers[nodeID] = storagerpc.Node{
		HostPort: net.JoinHostPort("localhost", strconv.Itoa(port)),
		NodeID:   ss.nodeID,
	}
	if numNodes == 1 {
		ss.isReady = true
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
		if !ss.isReady {
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

	if ss.isReady {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady

		// add to cluster
		if _, exisit := ss.servers[args.ServerInfo.NodeID]; !exisit {
			ss.servers[args.ServerInfo.NodeID] = args.ServerInfo
		}
		if len(ss.servers) == ss.numNodes {
			ss.isReady = true
			reply.Status = storagerpc.OK
			close(ss.readyChan)
		}
	}

	for k := range ss.servers {
		reply.Servers = append(reply.Servers, ss.servers[k])
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.isMaster {
		return errors.New("server is not a master")
	}

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

	reply.Status = generalReply.Status
	if reply.Status != storagerpc.OK {
		return nil
	}

	reply.Lease = generalReply.Lease
	reply.Value = value.(string)

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	generalReply, value := ss.generalGet(args)

	reply.Status = generalReply.Status
	if reply.Status != storagerpc.OK {
		return nil
	}

	reply.Lease = generalReply.Lease
	for e := value.(*list.List).Front(); e != nil; e = e.Next() {
		reply.Value = append(reply.Value, e.Value.(string))
	}

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

	ss.revokeLease(args.Key)
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
		ss.store[args.Key].(*list.List).PushFront(args.Value)
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

	ss.revokeLease(args.Key)
	l.PushFront(args.Value)
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
			ss.revokeLease(args.Key)
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
	successor := libstore.FindStorageServerId(key, ss.servers)
	if successor != ss.nodeID {
		return false
	}

	return true
}

// Get a key-value pair
func (ss *storageServer) generalGet(args *storagerpc.GetArgs) (*storagerpc.GetReply, interface{}) {
	status, ok := ss.assertKeyAndServer(args.Key)
	if !ok {
		return &storagerpc.GetReply{Status: status}, nil
	}

	ss.storeRWL.RLock()

	value, exisit := ss.store[args.Key]
	if !exisit {
		ss.storeRWL.RUnlock()
		return &storagerpc.GetReply{Status: storagerpc.KeyNotFound}, nil
	}

	reply := &storagerpc.GetReply{
		Status: storagerpc.OK,
	}

	if args.WantLease {
		// upgrade to write lock
		ss.storeRWL.RUnlock()
		ss.storeRWL.Lock()

		// refuse the lease request
		if ss.inRevoking[args.Key] {
			reply.Lease = storagerpc.Lease{Granted: false}
			ss.storeRWL.Unlock()
			return reply, value
		}

		// append a lease record
		leaseList := ss.leases[args.Key]
		if leaseList == nil {
			ss.leases[args.Key] = list.New()
			leaseList = ss.leases[args.Key]
		}

		leaseList.PushBack(&leaseRecord{
			expirationTime: time.Now().Add(time.Second * time.Duration(leaseSeconds)),
			hostport:       args.HostPort,
		})

		reply.Lease = storagerpc.Lease{
			Granted:      true,
			ValidSeconds: storagerpc.LeaseSeconds,
		}

		ss.storeRWL.Unlock()
		return reply, value
	}

	ss.storeRWL.RUnlock()
	return reply, value
}

// Revoke leases for the givin key from all lease holders.
// This func assume that it has already get a WLock
func (ss *storageServer) revokeLease(key string) {
	leaseList, exisit := ss.leases[key]
	if !exisit {
		return
	}

	ss.inRevoking[key] = true
	defer func() {
		delete(ss.inRevoking, key)
	}()
	done := make(chan *rpc.Call, 1)

	for e := leaseList.Front(); e != nil; e = e.Next() {
		lr := e.Value.(*leaseRecord)

		// not expired, need to revoke
		if lr.expirationTime.After(time.Now()) {
			duration := lr.expirationTime.Sub(time.Now())

			// release the lock to prevent blocking
			ss.storeRWL.Unlock()
			client, err := rpc.DialHTTP("tcp", lr.hostport)
			if err == nil {
				args := &storagerpc.RevokeLeaseArgs{Key: key}
				var reply storagerpc.RevokeLeaseReply
				client.Go("LeaseCallbacks.RevokeLease", args, &reply, done)
			}
			select {
			case <-time.After(duration):
			case <-done:
			}
			// acquire lock again
			ss.storeRWL.Lock()
		}
	}
	delete(ss.leases, key)
	return
}

func (ss *storageServer) joinCluster(masterServerHostPort string) error {
	for {
		master, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return err
		}

		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				HostPort: net.JoinHostPort("localhost", strconv.Itoa(ss.port)), // TODO: change localhost to real host name
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
