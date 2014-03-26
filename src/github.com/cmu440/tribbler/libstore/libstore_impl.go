package libstore

import (
	"errors"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

const (
	maximumTrials = 5
)

const (
	putCall int = iota
	appendToListCall
	removeFromListCall
	getCall
	getListCall
)

var (
	rpcNames = []string{
		"StorageServer.Put",
		"StorageServer.AppendToList",
		"StorageServer.RemoveFromList",
		"StorageServer.Get",
		"StorageServer.GetList",
	}
	rpcErrorString = []string{
		"StorageServer.Put return non-OK status: ",
		"StorageServer.AppendToList return non-OK status: ",
		"StorageServer.RemoveFromList return non-OK status: ",
		"StorageServer.Get return non-OK status: ",
		"StorageServer.GetList return non-OK status: ",
	}
)

type cachedItem struct {
	value          interface{} // value can be a string or a slice
	expirationTime time.Time
}

type libstore struct {
	myHostPort        string
	mode              LeaseMode
	storageServers    map[uint32]storagerpc.Node
	cache             map[string]cachedItem
	cacheRWL          sync.RWMutex
	storageRPCHandler map[uint32]*rpc.Client
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := &libstore{
		myHostPort:        myHostPort,
		mode:              mode,
		storageServers:    make(map[uint32]storagerpc.Node),
		cache:             make(map[string]cachedItem),
		storageRPCHandler: make(map[uint32]*rpc.Client),
	}

	// connect to the master server and get the server list
	master, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply
	ok := false
	for i := 0; i < maximumTrials; i++ {
		err = master.Call("StorageServer.GetServer", &args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			ok = true
			break
		}
		time.Sleep(time.Second)
	}

	if !ok {
		return nil, errors.New("Cannot get servers after " + strconv.Itoa(maximumTrials) + " trials")
	}

	// adding the server list
	for _, s := range reply.Servers {
		ls.storageServers[s.NodeID] = s
		ls.storageRPCHandler[s.NodeID], err = rpc.DialHTTP("tcp", s.HostPort)
		if err != nil {
			return nil, err
		}
	}

	// register the callback
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))

	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	v := ls.getFromCache(key)
	if v != nil {
		return v.(string), nil
	}

	// make args and reply
	var reply storagerpc.GetReply
	args := storagerpc.GetArgs{
		Key: key,
	}

	if ls.needLease(key) {
		args.WantLease = true
		args.HostPort = ls.myHostPort
	}

	// do rpc
	rpcHandler := ls.getStorageRPCHandler(key)
	err := rpcHandler.Call(rpcNames[getCall], &args, &reply)
	if err != nil {
		return "", err
	}

	if reply.Status != storagerpc.OK {
		return "", errors.New(rpcErrorString[getCall] + strconv.Itoa(int(reply.Status)))
	}

	if reply.Lease.Granted {
		ls.cacheRWL.Lock()

		ls.cache[key] = cachedItem{
			value:          reply.Value,
			expirationTime: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
		}

		ls.cacheRWL.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	return ls.generalPut(key, value, putCall)
}

func (ls *libstore) GetList(key string) ([]string, error) {
	v := ls.getFromCache(key)
	if v != nil {
		return v.([]string), nil
	}

	// make args and reply
	var reply storagerpc.GetListReply
	args := storagerpc.GetArgs{
		Key: key,
	}

	if ls.needLease(key) {
		args.WantLease = true
		args.HostPort = ls.myHostPort
	}

	// do rpc
	rpcHandler := ls.getStorageRPCHandler(key)
	err := rpcHandler.Call(rpcNames[getListCall], &args, &reply)
	if err != nil {
		return nil, err
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New(rpcErrorString[getListCall] + strconv.Itoa(int(reply.Status)))

	}

	// add to cache
	if reply.Lease.Granted {
		ls.cacheRWL.Lock()

		ls.cache[key] = cachedItem{
			value:          reply.Value,
			expirationTime: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
		}

		ls.cacheRWL.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	return ls.generalPut(key, removeItem, removeFromListCall)
}

func (ls *libstore) AppendToList(key, newItem string) error {
	return ls.generalPut(key, newItem, appendToListCall)
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.cacheRWL.Lock()
	defer ls.cacheRWL.Unlock()

	delete(ls.cache, args.Key)
	reply.Status = storagerpc.OK

	return nil
}

func FindStorageServerId(key string, servers map[uint32]storagerpc.Node) uint32 {
	// get username part
	index := strings.Index(key, ":")
	if index < 0 {
		panic("")
	}
	hash := StoreHash(key[0:index])

	var successor uint32
	distance := ^uint32(0)

	for id := range servers {
		tmpDistance := uint32(id - hash) // [*]auto overflow
		if tmpDistance < distance {
			distance = tmpDistance
			successor = id
		}
	}
	return successor
}

func (ls *libstore) getStorageRPCHandler(key string) *rpc.Client {
	nodeID := FindStorageServerId(key, ls.storageServers)
	return ls.storageRPCHandler[nodeID]
}

func (ls *libstore) generalPut(key, value string, callType int) error {
	rpcHandler := ls.getStorageRPCHandler(key)
	args := storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	var reply storagerpc.PutReply

	err := rpcHandler.Call(rpcNames[callType], &args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New(rpcErrorString[callType] + strconv.Itoa(int(reply.Status)))
	}
	return nil
}

func (ls *libstore) getFromCache(key string) interface{} {
	ls.cacheRWL.RLock()
	c, ok := ls.cache[key]
	if ok {
		if c.expirationTime.After(time.Now()) {
			return c.value
		}
		delete(ls.cache, key)
	}
	ls.cacheRWL.RUnlock()

	return nil
}

func (ls *libstore) needLease(key string) bool {
	if ls.mode == Never {
		return false
	}

	if ls.mode == Always {
		return true
	}

	// TODO
	return false
}
