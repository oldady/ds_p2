// A user is represented as "{userId}"
// User tribbles is list<Hash(timestamp, contents)> "{userId}:tbs"
// TribbleID is "{userId}:{Hash(timestamp, contents)}"
// TribbleValue format see below in const
// Subscription is <list> "{userId}:sbsp"
package tribserver

import (
	"container/heap"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
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
	subscribes, err := ts.Libstore.GetList(subscListKey)
	switch err {
	case nil:
		// we have successfully retrieved the list of subscribes
		switch mode {
		case doSubAppend:
			// check duplicate subscribes
			for _, subscribe := range subscribes {
				if subscribe == target {
					reply.Status = tribrpc.Exists
					return nil
				}
			}
		case doSubRemove:
			// check whether exist
			existTarget := false
			for _, subscribe := range subscribes {
				if subscribe == target {
					existTarget = true
					break
				}
			}
			if !existTarget {
				reply.Status = tribrpc.NoSuchTargetUser
				return nil
			}
		default:
			panic("")
		}
	case libstore.ErrorKeyNotFound:
		// This user hasn't done subscribe before. do thing
	default:
		return err
	}

	switch mode {
	case doSubAppend:
		err = ts.Libstore.AppendToList(subscListKey, target)
	case doSubRemove:
		err = ts.Libstore.RemoveFromList(subscListKey, target)
	}
	if err == nil { // ignore errors
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
	switch err {
	case nil:
	case libstore.ErrorKeyNotFound:
		userIDs = make([]string, 0)
	default:
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

	hashIds, err := ts.Libstore.GetList(tribListKey)
	switch err {
	case nil:
	case libstore.ErrorKeyNotFound:
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	default:
		return err
	}

	// reverse hash Id to achieve most recent tribbles first.
	for i, j := 0, len(hashIds)-1; i < j; i, j = i+1, j-1 {
		hashIds[i], hashIds[j] = hashIds[j], hashIds[i]
	}

	tribValues, err := ts.getTribValuesFromHashIds(user, hashIds)
	if err != nil { // ignore error
		// return err
	}

	reply.Tribbles = makeTribbles(user, tribValues)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) getTribValuesFromHashIds(user string, hashIds []string) ([]string, error) {
	var err error
	tribValues := make([]string, len(hashIds))

	var wg sync.WaitGroup

	for i, hashId := range hashIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tribValues[i], err = ts.Libstore.Get(makeTribId(user, hashId))
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()

	return tribValues, nil
}

func (ts *tribServer) getTribsFromSubs(subscList []string) ([]tribrpc.Tribble, error) {
	// get tribbles from all subscribe
	allTribValues := make([][]string, len(subscList))
	allTribNum := 0

	var wg sync.WaitGroup

	for i, target := range subscList {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hashIds, err := ts.Libstore.GetList(makeTribListKey(target))
			switch err {
			case nil:
			case libstore.ErrorKeyNotFound:
				allTribValues[i] = make([]string, 0)
			default:
				panic(err)
			}

			// reverse hash Id to achieve most recent tribbles first.
			for i, j := 0, len(hashIds)-1; i < j; i, j = i+1, j-1 {
				hashIds[i], hashIds[j] = hashIds[j], hashIds[i]
			}

			tribValues, err := ts.getTribValuesFromHashIds(target, hashIds)
			if err != nil {
				panic(err)
			}
			allTribNum += len(tribValues)
			allTribValues[i] = tribValues
		}()
	}
	wg.Wait()

	// get the most recent at most 100 tribbles.
	// use priority queue

	count := 0

	h := new(maxHeap)
	pos := make([]int, len(subscList))

	resLength := allTribNum
	if resLength > maxGetTribbleNum {
		resLength = maxGetTribbleNum
	}
	tribbles := make([]tribrpc.Tribble, resLength)

	for i, tribValues := range allTribValues {
		if pos[i] >= len(tribValues) {
			continue
		}
		item, err := makeHeapItem(tribValues[pos[i]], i)
		if err != nil {
			panic("")
		}
		heap.Push(h, item)
		pos[i]++
	}

	for count < len(tribbles) {
		item := (heap.Pop(h)).(*heapItem)
		tribbles[count] = makeTribble(subscList[item.index], item.value)
		count++

		i := item.index
		tribValues := allTribValues[i]
		if pos[i] < len(tribValues) {
			item, err := makeHeapItem(tribValues[pos[i]], i)
			if err != nil {
				panic("")
			}
			heap.Push(h, item)
			pos[i]++
		}
	}

	return tribbles, nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
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
	subscList, err := ts.Libstore.GetList(subscListKey)
	switch err {
	case nil:
	case libstore.ErrorKeyNotFound:
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	default:
		return err
	}

	tribbles, err := ts.getTribsFromSubs(subscList)
	if err != nil { // ignore error
		// return err
	}

	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
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

func makeTribble(user, tribValue string) tribrpc.Tribble {
	fields := strings.SplitN(tribValue, "\t", 2)
	if len(fields) != 2 {
		panic("")
	}
	nsec, err := strconv.Atoi(fields[0])
	if err != nil {
		panic("")
	}
	return tribrpc.Tribble{
		UserID:   user,
		Posted:   time.Unix(0, int64(nsec)),
		Contents: fields[1],
	}
}

func makeTribbles(user string, tribValues []string) []tribrpc.Tribble {
	resLength := len(tribValues)
	if resLength > maxGetTribbleNum {
		resLength = maxGetTribbleNum
	}
	tribbles := make([]tribrpc.Tribble, resLength)

	for i, tribValue := range tribValues {
		if i >= resLength {
			break
		}
		tribbles[i] = makeTribble(user, tribValue)
	}
	return tribbles
}

func makeHeapItem(tribValue string, index int) (*heapItem, error) {
	var err error

	hi := &heapItem{
		index: index,
		value: tribValue,
	}

	fields := strings.Split(tribValue, "\t")
	hi.priority, err = strconv.Atoi(fields[0])
	if err != nil {
		return nil, err
	}

	return hi, nil
}

// ************************
// **   PRIORITY QUEUE   **
// ************************

type heapItem struct {
	priority int
	index    int
	value    string
}

type maxHeap []*heapItem

func (h maxHeap) Len() int { return len(h) }
func (h maxHeap) Less(i, j int) bool {
	return h[i].priority > h[j].priority
}
func (h maxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *maxHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
