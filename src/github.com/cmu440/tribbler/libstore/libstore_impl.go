package libstore

import (
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var (
	ErrorKeyNotFound = errors.New("Key Not Found in Storage System")
)

type libstore struct {
	kvStore map[string]string
	klStore map[string]([]string)
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
		kvStore: make(map[string]string),
		klStore: make(map[string]([]string)),
	}
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	v, ok := ls.kvStore[key]
	if !ok {
		return "", ErrorKeyNotFound
	}
	return v, nil
}

func (ls *libstore) Put(key, value string) error {
	ls.kvStore[key] = value
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	l, ok := ls.klStore[key]
	if !ok {
		return nil, ErrorKeyNotFound
	}
	return l, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	l, ok := ls.klStore[key]
	if !ok {
		return nil
	}
	for i, item := range l {
		if item == removeItem {
			l = append(l[:i], l[i+1:]...)
			break
		}
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	l, ok := ls.klStore[key]
	if !ok {
		l = make([]string, 0)
		ls.klStore[key] = l
	}
	l = append(l, newItem)
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
