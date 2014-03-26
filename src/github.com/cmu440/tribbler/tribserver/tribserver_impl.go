// A user is represented as "{userId}"
// User tribbles is list<Hash(timestamp, contents)> "{userId}:tbs"
// Tribble is "{userId}:{Hash(timestamp, contents)}"
// - "%s\t%s" (time.Now().Unix(), contents)
// Subscription is <list> "{userId}:sbsp"
package tribserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

var _ = fmt.Printf

type tribServer struct {
	// TODO: implement this!
	libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	newStore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, err
	}

	ts := &tribServer{
		Libstore: newStore,
	}

	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		return nil, err
	}

	rpc.HandleHTTP()
	l, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	go http.Serve(l, nil)

	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	_, err := ts.Libstore.Get(args.UserID)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	switch err {
	case libstore.ErrorKeyNotFound: // expected error
	default:
		return err
	}

	err = ts.Libstore.Put(args.UserID, "")
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	user := args.UserID
	target := args.TargetUserID

	_, err := ts.Libstore.Get(user)
	switch err {
	case nil: // expected case
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchUser
		return nil
	default:
		return err
	}

	_, err = ts.Libstore.Get(target)
	switch err {
	case nil: // expected case
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	default:
		return err
	}

	subscriptionList := fmt.Sprintf("%s:sbsp", user)
	err = ts.Libstore.AppendToList(subscriptionList, target)
	if err != nil {
		return err
	}

	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
