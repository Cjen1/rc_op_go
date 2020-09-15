package rc_op_go

import (
	conn "github.com/Cjen1/rc_op_go/capnp_go_conn"
	api "github.com/Cjen1/ocamlpaxos_api"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	capnp "zombiezen.com/go/capnproto2"
)

type resultPromise struct {
	request *capnp.Message
	waiter  chan *api.ClientResponse
}

type mapStruct struct {
	prom *resultPromise
	msg *capnp.Message
}

type Client struct {
	cid           int64
	reqId         *int64
	dispatchChans []chan *capnp.Message
	pool          *sync.Pool
	promiseMap    *sync.Map
	retryTimeout  time.Duration
	recvSel       []reflect.SelectCase
	addedConn     chan reflect.SelectCase
}

func (cli *Client) dispatch(msg *capnp.Message) {
	for _, ch := range cli.dispatchChans {
		select {
		case ch <- msg:
		default: // required to ensure that no blocking occurs
		}
	}
}

func (cli *Client) addConnection(c *conn.PersistConn) {
	{
		ch := make(chan *capnp.Message, 128)
		cli.dispatchChans = append(cli.dispatchChans, ch)
		go func(ch chan *capnp.Message) {
			for msg := range ch {
				c.Write(msg)
			}
		}(ch)
	}

	{
		recvCh := make(chan *capnp.Message, 2)
		recvCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(recvCh),
		}

		go func(ch chan *capnp.Message, c *conn.PersistConn) {
			for true {
				msg := c.Read()
				ch <- msg
			}
		}(recvCh, c)

		cli.addedConn <- recvCase
	}
}

func (cli *Client) send(id int64, msg *capnp.Message) *api.ClientResponse {
	prom := cli.pool.Get().(*resultPromise)
	prom.request = msg
	ms := mapStruct{prom:prom, msg:msg}
	cli.promiseMap.Store(id, &ms)
	cli.dispatch(msg)
	return <-prom.waiter
}

func resolver_loop(cli *Client) {
	for true {
		i, msg_pointer, ok := reflect.Select(cli.recvSel)
		if i == 0 { //new conn added => reset loop
			sel, _ := msg_pointer.Interface().(reflect.SelectCase)
			cli.recvSel = append(cli.recvSel, sel)
			log.Printf("new select case added, restarting loop)")
			continue
		}
		if !ok {
			panic("receiving channel closed...")
		}
		//must be a received value
		root, err := api.ReadRootServerMessage(msg_pointer.Interface().(*capnp.Message))
		if err != nil {
			log.Printf("Could not parse error")
			panic(err)
		}
		log.Printf(root.String())
		msg, _ := root.ClientResponse()
		ms_intf, ok := cli.promiseMap.Load(msg.Id())
		if !ok {
			//Promise doesn't exist in map => is resolved already
			continue
		}
		ms := ms_intf.(*mapStruct)
		prom := *ms.prom
		log.Printf("Removing %d", msg.Id())
		cli.promiseMap.Delete(msg.Id())
		prom.waiter <- &msg
		log.Printf("Resolved %d", msg.Id())
	}
}

func (cli *Client) retry_loop () {
	for true {
		time.Sleep(cli.retryTimeout)
		cli.promiseMap.Range(func (key, value interface{}) bool {
			cli.dispatch(value.(*mapStruct).msg)
			log.Printf("Retried %d", key)
			return true
		})
	}
}

func (cli *Client) getId() int64 {
	id := atomic.AddInt64(cli.reqId, 1)
	rid := id + cli.cid * 100000
	return rid
}

func (cli *Client) Write(key []byte, data []byte) *api.ClientResponse {
	cmsg, seg, _ := capnp.NewMessage(capnp.MultiSegment(nil))
	root, _ := api.NewRootServerMessage(seg)
	resp, _ := root.NewClientRequest()
	id := cli.getId()
	resp.SetId(id)
	resp.SetKey(key)
	resp.SetWrite(data)
	return cli.send(id, cmsg)
}

func (cli *Client) Read(key []byte) *api.ClientResponse {
	cmsg, seg, _ := capnp.NewMessage(capnp.MultiSegment(nil))
	root, _ := api.NewRootServerMessage(seg)
	resp, _ := root.NewClientRequest()
	id := cli.getId()
	resp.SetId(id)
	resp.SetKey(key)
	resp.SetRead()
	return cli.send(id, cmsg)
}

func (cli *Client) AddConnection(addr string) {
	c := conn.Create_PersistConn(addr, cli.cid)
	cli.addConnection(c)
}

func Create(cid int64) *Client {
	pool := &sync.Pool{
		New: func() interface{} {
			return &resultPromise{
				request: nil,
				waiter:  make(chan *api.ClientResponse),
			}
		},
	}
	var pMap sync.Map

	recvSel := make([]reflect.SelectCase, 0)
	newConn := make(chan reflect.SelectCase)
	newConnSel := reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(newConn),
	}
	recvSel = append(recvSel, newConnSel)
	reqId := int64(0)
	res := &Client{
		cid:           cid,
		reqId:         &reqId,
		dispatchChans: make([]chan *capnp.Message, 0),
		pool:          pool,
		promiseMap:    &pMap,
		retryTimeout:  500 * time.Millisecond,
		recvSel:       recvSel,
		addedConn:     newConn,
	}

	go resolver_loop(res)
	go res.retry_loop()
	return res
}
