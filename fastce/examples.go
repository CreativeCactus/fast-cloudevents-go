package fastce

import (
	"fmt"
	"log"
	"net"
	"net/http"

	j "github.com/creativecactus/fast-cloudevents-go/jsonce"

	"github.com/valyala/fasthttp"
)

// ExampleCEClientCEServerImplementation shows a predictable example using ExampleCEClientCEServer
func ExampleCEClientCEServerImplementation() {
	ces := []j.CloudEvent{
		j.CloudEvent{
			Source: "Example",
		},
	}
	mode := j.ModeBinary

	ces, err := ExampleCEClientCEServer(ces, mode)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("Received: %d, first has Source:%s\n", len(ces), ces[0].Source)
	// Output: Received: 1, first has Source:Example
}

// ExampleCEClientCEServer shows an example of using both a CEClient and CEServer together
// Notice that some pointer shuffling is needed to have the client response (inbound) point
// to the server response (outbound). This is where HTTP would usually sit.
// This is not needed on the requesting side because of how RequestCtx is used.
func ExampleCEClientCEServer(ces []j.CloudEvent, mode j.Mode) (result []j.CloudEvent, err error) {
	// Init client
	cec, err := NewCEClient("PUT", "")
	if err != nil {
		err = fmt.Errorf("Example failed to Init: %s", err.Error())
		return
	}
	defer cec.Release()

	// Client send request to server
	err = cec.SendEvents(j.DefaultCEToMap, ces, mode)
	if err != nil {
		err = fmt.Errorf("Example failed to Send: %s", err.Error())
		return
	}

	// Server receive and respond
	// err = cec.Send() // No actual HTTP request in this example
	ces, mode, err = GetEvents(j.DefaultMapToCE, cec.Request)
	if err != nil {
		err = fmt.Errorf("Example failed to Get: %s", err.Error())
		return
	}
	if len(ces) == 0 {
		err = fmt.Errorf("Example Get returned 0 events")
		return
	}

	err = SetEvents(j.DefaultCEToMap, cec.Response, ces, mode)
	if err != nil {
		err = fmt.Errorf("Example failed to Set: %s", err.Error())
		return
	}

	// Client receive response
	ces, mode, err = cec.RecvEvents(j.DefaultMapToCE)
	if err != nil {
		err = fmt.Errorf("Example failed to Recv: %s", err.Error())
		return
	}

	result = ces
	return
}

// SimpleServer shows the highest level interface for fastce, as well as a mechanism for low level configuration
// listenAddr should be an interface:port such as 0.0.0.0:0. If port is 0, next available free port is used
func SimpleServer(listenAddr string) error {
	// An example of a custom unmarshal function
	MyMapToCE := func(cm j.CEMap) (ce j.CloudEvent, err error) {
		// In this example, we still want to perform the DefaultCEToMap validation
		// But we will automatically generate an ID if it is not present
		if id, ok := cm["id"].(string); !ok || len(id) < 1 {
			cm["id"] = "SomeRandomRuntimeGeneratedID"
		}
		return j.DefaultMapToCE(cm)
	}

	handler := func(ces j.CloudEvents) (res j.CloudEvents, err error) {
		// This is a simple echo server
		res = ces
		return
	}

	// In cases where HTTP level actions need to be taken (setting headers, routing), use ListenAndServeHTTP
	// In cases where an external server is used and you don't want to pass the request down to this server,
	// you can use the fastce.Get*/Set* functions directly (Send*/Recv* for clients).
	return CEServer{}.ListenAndServeCE(listenAddr, j.DefaultCEToMap, MyMapToCE, handler)
}

// ExampleServer shows an example implementation with a fasthttp server.
// listenAddr should be an interface:port such as 0.0.0.0:0. If port is 0, next available free port is used
// handler is a function to handle fasthttp.RequestCtx, such as ExampleHandler
// Returns the server created, a channel for receiving fatal errors (nil if .Shutdown() gracefully),
// the address used to listen (useful if the provided listenAddr has a 0 port), any init error.
func ExampleServer(listenAddr string, handler func(ctx *fasthttp.RequestCtx)) (server *fasthttp.Server, shutdownErr <-chan error, addr string, err error) {
	server = &fasthttp.Server{
		Handler: handler,
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		err = fmt.Errorf("Listen error: %s", err.Error())
		return
	}
	listenAddr = fmt.Sprintf("http://%s", listener.Addr().String())

	shutdown := make(chan error)
	log.Printf("Listening on %s", listenAddr)
	go func() {
		err := server.Serve(listener)
		shutdown <- err
	}()
	shutdownErr = shutdown
	return server, shutdownErr, listenAddr, nil
}

// ExampleHandler shows an example implementation of a fasthttp requestCtx handler.
// It responds with a hard coded string if the url is /info (any method)
// Otherwise it acts as a CloudEvents echo server.
func ExampleHandler(ctx *fasthttp.RequestCtx) {
	switch p := string(ctx.Path()); p {
	case "/info":
		// If we had a storage interface here, we could show
		// a projected count of events stored, for example.
		ctx.Write([]byte("Example Server"))
		break
	default:
		ces, mode, err := GetEvents(j.DefaultMapToCE, &ctx.Request)
		if err != nil {
			log.Printf("ERR: %s", err.Error())
			ctx.Error(err.Error(), http.StatusBadRequest)
			return
		} else {
			log.Printf("OK : Received %d events in mode %d\n", len(ces), mode)
		}
		// log.Printf("\tData: %#v\n", ces)
		SetEvents(j.DefaultCEToMap, &ctx.Response, ces, mode)
	}
}
