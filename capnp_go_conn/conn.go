package capnp_go_conn

import (
	"bufio"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"

	"zombiezen.com/go/capnproto2"
)

type PersistConn struct {
	addr            string
	cid             int64
	conn            net.Conn
	encoder_channel chan *capnp.Message
	w               *bufio.Writer
	fch             chan struct{}
	decoder_channel chan *capnp.Message
	reset           chan bool
	sync.Mutex
}

func connect(addr string, cid int64) net.Conn {
	for true {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			etx := binary.Write(conn, binary.BigEndian, &cid)
			if etx == nil {
				log.Printf("Connected to %s", addr)
				return conn
			} else {
				log.Printf("Failed to connect to %s with %s", addr, etx.Error())
			}
		}
		log.Printf("Failed to connect to %s", addr)
		time.Sleep(5 * time.Second)
	}
	panic("Should not be reachable")
}

func encoder_loop(c *PersistConn, encoder *capnp.Encoder) {
	for msg := range c.encoder_channel {
		c.Lock()
		err := encoder.Encode(msg)
		c.Unlock()
		if err != nil {
			log.Printf("Failed to encode with %s", err.Error())
			c.reconnect_PersistConn()
			break
		}
	}
}

func decoder_loop(c *PersistConn, decoder *capnp.Decoder) {
	for true {
		msg, err := decoder.Decode()
		if err != nil {
			log.Printf("Failed to decode with %s",err.Error())
			c.reconnect_PersistConn()
			break
		}
		c.decoder_channel <- msg
	}
}

func (c *PersistConn) setup(addr string, cid int64) {
	c.addr = addr
	c.cid = cid

	log.Printf("setup")
	conn := connect(addr, cid)
	c.conn = conn

	if (c.encoder_channel == nil && c.decoder_channel == nil) {
		log.Printf("setup Making chans")
		c.encoder_channel = make(chan *capnp.Message)
		c.decoder_channel = make(chan *capnp.Message)
	}

	log.Printf("setup Making buffered writer")
	c.w = bufio.NewWriter(conn)
	c.fch = make(chan struct{}, 1024)
	go func() {
		for {
			if _, ok := <-c.fch; !ok {
				return
			}
			c.Lock()
			if c.w.Buffered() > 0 {
				c.w.Flush()
				log.Printf("Flushing")
			}
			c.Unlock()
		}
	}()

	encoder := capnp.NewEncoder(c.w)
	go encoder_loop(c, encoder)

	log.Printf("setup Making buffered reader")
	brd := bufio.NewReader(conn)
	decoder := capnp.NewDecoder(brd)
	go decoder_loop(c, decoder)

	log.Printf("Sending reset")
	reset := c.reset
	c.reset = make(chan bool)
	close(reset)
	log.Printf("Sent reset")
}

func (c *PersistConn) reconnect_PersistConn() {
	c.Lock()
	defer c.Unlock()

	log.Printf("Reconnecting")

	//Close and nil the channels
	close(c.fch)
	c.fch = nil

	c.conn.Close()
	c.conn = nil

	c.setup(c.addr, c.cid)
}

func Create_PersistConn(addr string, cid int64) *PersistConn {
	c := &PersistConn{reset : make(chan bool)}
	c.setup(addr, cid)

	return c
}

func (c *PersistConn) Write(msg *capnp.Message) {
	c.encoder_channel <- msg
	if len(c.fch) == 0 {
		select {
		case c.fch <- struct{}{}:
		default:
		}
	}
}

func (c *PersistConn) Read() (msg *capnp.Message) {
	for true {
		select {
		case msg, ok := <-c.decoder_channel:
			if ok {
				return msg
			} else {
				continue
			}
		case <-c.reset:
			continue
		}
	}
	panic("Should not be reachable")
}
