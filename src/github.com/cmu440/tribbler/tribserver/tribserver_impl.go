// A user is represented as "{userId}"
// User tribbles is list<Hash(timestamp, contents)> "{userId}:tbs"
// TribbleID is "{userId}:{Hash(timestamp, contents)}"
// TribbleValue format see below in const
// Subscription is <list> "{userId}:sbsp"
package tribserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

var _ = fmt.Printf

const (
	maxGetTribbleNum   = 100
	tribListKeySuffix  = "tbs"
	subscListKeySuffix = "sbsp"
	tribbleValueFormat = "%d\t%s" // (time.Now().UnixNano(), contents)
)

const (
	doSubAppend uint8 = iota + 1
	doSubRemove
)

type tribServer struct {
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
	case libstore.ErrorKeyNotFound: // expected error, do nothing
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

func (ts *tribServer) doSub(user, target string, reply *tribrpc.SubscriptionReply, mode uint8) error {
	_, err := ts.Libstore.Get(user)
	switch err {
	case nil: // expected case, do nothing
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchUser
		return nil
	default:
		return err
	}

	_, err = ts.Libstore.Get(target)
	switch err {
	case nil: // expected case, do nothing
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	default:
		return err
	}

	subscListKey := makeSubscListKey(user)
	switch mode {
	case doSubAppend:
		err = ts.Libstore.AppendToList(subscListKey, target)
	case doSubRemove:
		err = ts.Libstore.RemoveFromList(subscListKey, target)
	}
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return ts.doSub(
		args.UserID,
		args.TargetUserID,
		reply,
		doSubAppend,
	)
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return ts.doSub(
		args.UserID,
		args.TargetUserID,
		reply,
		doSubRemove,
	)
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	user := args.UserID
	_, err := ts.Libstore.Get(user)
	switch err {
	case nil: // expected case, do nothing
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchUser
		return nil
	default:
		return err
	}

	subscListKey := makeSubscListKey(user)
	userIDs, err := ts.Libstore.GetList(subscListKey)
	if err != nil {
		return err
	}
	reply.UserIDs = userIDs
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	user := args.UserID

	_, err := ts.Libstore.Get(user)
	switch err {
	case nil: // expected case, do nothing
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchUser
		return nil
	default:
		return err
	}

	tribValue := makeTribValue(args.Contents)

	tribHash := makeTribHash(tribValue)
	tribListKey := makeTribListKey(user)

	// insert hash value as tribble ID to user tribbles list
	err = ts.Libstore.AppendToList(tribListKey, tribHash)
	if err != nil {
		return err
	}

	// then insert mapping from unique Id in each user to tribble value
	tribId := makeTribId(user, tribHash)

	err = ts.Libstore.Put(tribId, tribValue)
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	user := args.UserID

	_, err := ts.Libstore.Get(user)
	switch err {
	case nil: // expected case, do nothing
	case libstore.ErrorKeyNotFound:
		reply.Status = tribrpc.NoSuchUser
		return nil
	default:
		return err
	}

	tribListKey := makeTribListKey(user)

	tribHashIdList, err := ts.Libstore.GetList(tribListKey)
	if err != nil {
		return err
	}

	tribValues := make([]string, len(tribHashIdList))
	for i, hashId := range tribHashIdList {
		tribValues[i], err = ts.Libstore.Get(makeTribId(user, hashId))
		if err != nil {
			return err
		}
	}

	reply.Tribbles = makeTribbles(user, tribValues)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func makeSubscListKey(user string) string {
	return fmt.Sprintf("%s:%s", user, subscListKeySuffix)
}

func makeTribId(user, hashId string) string {
	return fmt.Sprintf("%s:%s", user, hashId)
}

func makeTribValue(contents string) string {
	return fmt.Sprintf(tribbleValueFormat, time.Now().UnixNano(), contents)
}

func makeTribHash(tribValue string) string {
	return fmt.Sprintf("%d", libstore.StoreHash(tribValue))
}

func makeTribListKey(user string) string {
	return fmt.Sprintf("%s:%s", user, tribListKeySuffix)
}

func makeTribbles(user string, tribValues []string) []tribrpc.Tribble {
	resLength := len(tribValues)
	if resLength > maxGetTribbleNum {
		resLength = maxGetTribbleNum
	}
	tribbles := make([]tribrpc.Tribble, resLength)

	for i, tribValue := range tribValues {
		if i >= maxGetTribbleNum {
			break
		}
		fields := strings.SplitN(tribValue, "\t", 2)
		if len(fields) != 2 {
			panic("")
		}
		nsec, err := strconv.Atoi(fields[0])
		if err != nil {
			panic("")
		}
		tribbles[i] = tribrpc.Tribble{
			UserID:   user,
			Posted:   time.Unix(0, int64(nsec)),
			Contents: fields[1],
		}
	}
	return tribbles
}
