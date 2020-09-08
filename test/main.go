package main

import (
	api "github.com/Cjen1/rc_op_go"
	client_api "github.com/Cjen1/rc_op_go/client_api"
	"log"
	"sync"
)

func do(client *api.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	res := client.Write([]byte("test"), []byte("test1"))
	switch (res.Which()) {
	case client_api.ClientResponse_Which_success :
		log.Printf("Performed Write")
	default:
		log.Printf("Received %s instead of %s", res.String(), "success")
		panic("Received incorrect response")
	}
	log.Printf("Performed Write")
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	cli := api.Create(101)
	cli.AddConnection("127.0.0.1:5000")
	var wg sync.WaitGroup
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		do(cli, &wg)
	}
	wg.Wait()
}
