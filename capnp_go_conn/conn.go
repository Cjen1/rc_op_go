package main

import (
	"encoding/binary"
	"log"
	"net"
	"time"

	"zombiezen.com/go/capnproto2"
)

type PersistConn struct {
	addr            string
	cid             int64
	conn            net.Conn
	encoder_channel chan *capnp.Message
	decoder_channel chan *capnp.Message
	reset           chan bool
}

func (c *PersistConn) Read() (msg *capnp.Message) {
	for true {
		select {
		case msg := <-c.decoder_channel:
			return msg
		case <-c.reset:
			continue
		}
	}
	panic("Should not be reachable")
}

func (c *PersistConn) Write(msg *capnp.Message) {
	log.Printf("Pushing msg to write channel")
	c.encoder_channel <- msg
}

func encoder_loop(c *PersistConn, encoder *capnp.Encoder) {
	for msg := range c.encoder_channel {
		log.Printf("Got another msg to encode")
		err := encoder.Encode(msg)
		if err != nil {
			panic(err)
			log.Printf("Failed to encode")
			close(c.encoder_channel)
			c.reconnect_PersistConn()
			break
		}
		log.Printf("Encoded successfully")
	}
}

func decoder_loop(c *PersistConn, decoder *capnp.Decoder) {
	for true {
		log.Printf("Waiting for next msg")
		msg, err := decoder.Decode()
		if err != nil {
			panic(err)
			log.Printf("Failed to encode")
			close(c.decoder_channel)
			c.reconnect_PersistConn()
			break
		}
		log.Printf("decoded successfully")
		c.decoder_channel <- msg
	}
}

func connect(addr string, cid int64) net.Conn {
	for true {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			etx := binary.Write(conn, binary.BigEndian, &cid)
			log.Printf("Connected to %s", addr)
			if etx == nil {
				return conn
			}
		}
		log.Printf("Failed to connect to %s", addr)
		time.Sleep(5 * time.Second)
	}
	panic("Should not be reachable")
}

func (c *PersistConn) dispatch_loops(conn net.Conn) {
	encoder := capnp.NewEncoder(conn)
	go encoder_loop(c, encoder)

	decoder := capnp.NewDecoder(conn)
	go decoder_loop(c, decoder)
}

func (c *PersistConn) reconnect_PersistConn() {
	log.Printf("Reconnecting")
	c.encoder_channel = nil
	c.decoder_channel = nil
	c.conn.Close()
	c.conn = nil

	c.encoder_channel = make(chan *capnp.Message)
	c.decoder_channel = make(chan *capnp.Message)

	conn := connect(c.addr, c.cid)
	c.conn = conn
	c.dispatch_loops(conn)
	c.reset <- true
}

func create_PersistConn(addr string, cid int64) *PersistConn {
	conn := connect(addr, cid)

	c := &PersistConn{
		addr:            addr,
		conn:            conn,
		encoder_channel: make(chan *capnp.Message),
		decoder_channel: make(chan *capnp.Message),
		reset:           make(chan bool),
	}

	c.dispatch_loops(conn)

	return c
}
