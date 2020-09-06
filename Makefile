
build: ocamlpaxos/lib/messaging_api.capnp
	capnp compile -I /home/cjj39/go/src/zombiezen.com/go/capnproto2/std -ogo:client_api --src-prefix=ocamlpaxos/lib/ ocamlpaxos/lib/messaging_api.capnp
	go build client.go
