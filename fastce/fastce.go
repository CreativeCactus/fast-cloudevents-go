package fastce

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	j "github.com/creativecactus/fast-cloudevents-go/jsonce"

	"github.com/valyala/fasthttp"
)

// DefaultContentType is used for events which do not have an associated DataContentType and one cannot be inferred from encoding (likely because it is binary).
const DefaultContentType = "text/plain"

// DefaultContentTypeJSON is used for events which do not have an associated DataContentType and one can be inferred (as DefaultContentTypeJSON).
const DefaultContentTypeJSON = "application/json"

/*
 ██████╗  ██████╗ ██████╗  ██████╗███████╗██╗      █████╗ ██╗███╗   ██╗
 ██╔══██╗██╔═══██╗██╔══██╗██╔════╝██╔════╝██║     ██╔══██╗██║████╗  ██║
 ██████╔╝██║   ██║██████╔╝██║     █████╗  ██║     ███████║██║██╔██╗ ██║
 ██╔═══╝ ██║   ██║██╔══██╗██║     ██╔══╝  ██║     ██╔══██║██║██║╚██╗██║
 ██║     ╚██████╔╝██║  ██║╚██████╗███████╗███████╗██║  ██║██║██║ ╚████║
 ╚═╝      ╚═════╝ ╚═╝  ╚═╝ ╚═════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝
*/

// CEServer is a convenience wrapper around GetEvents[Ctx] and SetEvents[Ctx]
type CEServer struct {
	// Ctx *fasthttp.RequestCtx
	Listener net.Listener // Optional, if an external listener is used by the server
	Server   *fasthttp.Server
	Address  string // For reading back the bound address, in case it was changed (eg. port=0)
}

// ListenAndServe simply sets up the underlying server and net.Listener
// You can also call srv.Server.ListenAndServe() directly if using your own server
// This will overwrite the Server and Listener
func (srv CEServer) ListenAndServeHTTP(addr string, handler func(*fasthttp.RequestCtx)) (err error) {
	srv.Server = &fasthttp.Server{
		Handler: handler,
	}
	if srv.Listener, err = net.Listen("tcp", addr); err != nil {
		err = fmt.Errorf("Listener failed: %s", err.Error())
		return
	}
	srv.Address = fmt.Sprintf("http://%s", srv.Listener.Addr().String())
	return srv.Server.Serve(srv.Listener)
}

// ListenAndServeCE simply sets up the underlying server and net.Listener with a default HTTP handler
// You can also call srv.Server.ListenAndServe() directly if using your own server
// This will overwrite the Server and Listener
func (srv CEServer) ListenAndServeCE(addr string, CEToMap j.CEToMap, MapToCE j.MapToCE, handler func(j.CloudEvents) (j.CloudEvents, error)) (err error) {
	return srv.ListenAndServeHTTP(addr, func(ctx *fasthttp.RequestCtx) {
		ces, mode, err := GetEventsCtx(MapToCE, ctx)
		if err != nil {
			err = fmt.Errorf("Get Events: %s", err.Error())
			ctx.Error(err.Error(), fasthttp.StatusInternalServerError) // Overwrites any body/headers
			return
		}

		ces, err = handler(ces)
		if err != nil {
			err = fmt.Errorf("Handle Events: %s", err.Error())
			ctx.Error(err.Error(), fasthttp.StatusInternalServerError) // Overwrites any body/headers
			return
		}

		if len(ces) > 0 {
			err = SetEventsCtx(CEToMap, ctx, ces, mode)
			if err != nil {
				err = fmt.Errorf("Set Events: %s", err.Error())
				ctx.Error(err.Error(), fasthttp.StatusInternalServerError) // Overwrites any body/headers
				return
			}
		} else {
			ctx.SuccessString(mode.ContentTypePlus("json"), "Success")
		}
		return
	})
}

// CEClient is a convenience wrapper around SendEvents and RecvEvents
type CEClient struct {
	Request  *fasthttp.Request
	Response *fasthttp.Response
	Released bool
	Client   *fasthttp.HostClient
}

// NewCEClient creates a CEClient for a given URI and method
func NewCEClient(method, URLString string) (cec CEClient, err error) {
	URL, err := url.Parse(URLString)
	if err != nil {
		err = fmt.Errorf("Could not parse %s as URL (scheme://host:port/path): %s", URLString, err.Error())
		return
	}

	if host := URL.Hostname(); j.InSlice(host, []string{
		"",
		"::",
		"[::]",
		"0.0.0.0",
	}) {
		if port := URL.Port(); len(port) > 0 {
			URL.Host = fmt.Sprintf("localhost:%s", port)
		} else {
			URL.Host = "localhost"
		}
	}

	c := &fasthttp.HostClient{
		Addr: URL.Host,
	}
	c.DialDualStack = true // Allow IPv6
	c.MaxConns = 10

	cec = CEClient{
		Request:  fasthttp.AcquireRequest(),
		Response: fasthttp.AcquireResponse(),
		Released: false,
		Client:   c,
	}

	cec.Request.SetRequestURI(URL.String())
	cec.Request.Header.SetMethod(method)
	return
}

// Send performs the underlying request of the CEClient
// It should be called after calling .SendCE
func (cec *CEClient) Send() (err error) {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	err = cec.Client.DoTimeout(cec.Request, cec.Response, 30*time.Second)
	if err != nil {
		err = fmt.Errorf("HTTP Error: %s", err.Error())
	}
	return err
}

// Release must be called to GC the request and response
func (cec *CEClient) Release() {
	if !cec.Released {
		cec.Released = false
		fasthttp.ReleaseRequest(cec.Request)
		fasthttp.ReleaseResponse(cec.Response)
	}
}

// SendEvents allows sending CloudEvents to the server
func (cec *CEClient) SendEvents(mapper j.CEToMap, ces []j.CloudEvent, mode j.Mode) error {
	return SendEvents(mapper, cec.Request, ces, mode)
}

// RecvEvents allows receiving CloudEvents in the server response
func (cec *CEClient) RecvEvents(mapper j.MapToCE) (ces []j.CloudEvent, mode j.Mode, err error) {
	return RecvEvents(mapper, cec.Response)
}

/*
 ███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗      ██╗███╗   ██╗
 ██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗     ██║████╗  ██║
 ███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝     ██║██╔██╗ ██║
 ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗     ██║██║╚██╗██║
 ███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║     ██║██║ ╚████║
 ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝     ╚═╝╚═╝  ╚═══╝

*/

// GetEventsCtx receives cloudevents from a Request in any mode
func GetEventsCtx(mapper j.MapToCE, ctx *fasthttp.RequestCtx) (ces []j.CloudEvent, mode j.Mode, err error) {
	return GetEvents(mapper, &ctx.Request)
}

// GetEvents receives cloudevents from a Request in any mode
func GetEvents(mapper j.MapToCE, req *fasthttp.Request) (ces []j.CloudEvent, mode j.Mode, err error) {
	// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#13-content-modes
	rr := ReqResFromReq(req)

	if mode, err = rr.GetMode(); err != nil {
		err = fmt.Errorf("Could not get mode: %s", err.Error())
		return
	}

	switch mode {
	case j.ModeBinary:
		ce, err := rr.BinaryToCE(mapper)
		if err != nil {
			return ces, mode, fmt.Errorf("Could not get binary event: %s", err.Error())
		}
		ces = append(ces, ce)
		return ces, mode, nil
	case j.ModeStructure:
		// Determine the media type with which to parse the event
		// Or reject anything other than JSON
		// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#3-http-message-mapping
		ct := string(req.Header.Peek("Content-Type"))
		if !strings.HasPrefix(ct, mode.ContentTypePlus("json")) {
			return ces, mode, fmt.Errorf("Unknown event content media type: %s", ct)
		}

		ce, err := rr.StructureJSONToCE(mapper)
		if err != nil {
			return ces, mode, fmt.Errorf("Could not get structure event: %s", err.Error())
		}
		ces = append(ces, ce)
		return ces, mode, nil
	case j.ModeBatch:
		// Determine the media type with which to parse the events
		// Or reject anything other than JSON
		// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#3-http-message-mapping
		ct := string(req.Header.Peek("Content-Type"))
		if !strings.HasPrefix(ct, mode.ContentTypePlus("json")) {
			return ces, mode, fmt.Errorf("Unknown event content media type: %s", ct)
		}

		ces, err = rr.BatchJSONToCE(mapper)
		if err != nil {
			err = fmt.Errorf("Could not get batch events: %s", err.Error())
		}
		return ces, mode, err
	default:
		err = fmt.Errorf("Unknown mode: %d", mode)
	}
	return
}

/*
 ███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗       ██████╗ ██╗   ██╗████████╗
 ██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗     ██╔═══██╗██║   ██║╚══██╔══╝
 ███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝     ██║   ██║██║   ██║   ██║
 ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗     ██║   ██║██║   ██║   ██║
 ███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║     ╚██████╔╝╚██████╔╝   ██║
 ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝      ╚═════╝  ╚═════╝    ╚═╝
*/

// SetEventsCtx accepts the mode and content of a response and puts any event(s) into it
// Note that ces[1...] are dropped unless mode is batch
func SetEventsCtx(mapper j.CEToMap, ctx *fasthttp.RequestCtx, ces []j.CloudEvent, mode j.Mode) (err error) {
	return SetEvents(mapper, &ctx.Response, ces, mode)
}

// SetEvents accepts the mode and content of a response and puts any event(s) into it
// Note that ces[1...] are dropped unless mode is batch
func SetEvents(mapper j.CEToMap, res *fasthttp.Response, ces []j.CloudEvent, mode j.Mode) (err error) {
	if len(ces) < 1 && mode != j.ModeBatch {
		return fmt.Errorf("Could not put %d events in mode %d", len(ces), mode)
	}

	rr := ReqResFromRes(res)
	// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#13-content-modes
	switch mode {
	case j.ModeBinary:
		err := rr.CEToBinary(mapper, ces[0])
		if err != nil {
			return fmt.Errorf("Could not set binary event: %s", err.Error())
		}
		return nil
	case j.ModeStructure:
		err := rr.CEToStructureJSON(mapper, ces[0])
		if err != nil {
			return fmt.Errorf("Could not set structure event: %s", err.Error())
		}
		return nil
	case j.ModeBatch:
		err := rr.CEToBatchJSON(mapper, ces)
		if err != nil {
			return fmt.Errorf("Could not set batch events: %s", err.Error())
		}
		return nil
	default:
		err = fmt.Errorf("Unknown mode: %d", mode)
	}
	return
}

/*
  ██████╗██╗     ██╗███████╗███╗   ██╗████████╗      ██████╗ ██╗   ██╗████████╗
 ██╔════╝██║     ██║██╔════╝████╗  ██║╚══██╔══╝     ██╔═══██╗██║   ██║╚══██╔══╝
 ██║     ██║     ██║█████╗  ██╔██╗ ██║   ██║        ██║   ██║██║   ██║   ██║
 ██║     ██║     ██║██╔══╝  ██║╚██╗██║   ██║        ██║   ██║██║   ██║   ██║
 ╚██████╗███████╗██║███████╗██║ ╚████║   ██║        ╚██████╔╝╚██████╔╝   ██║
  ╚═════╝╚══════╝╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝         ╚═════╝  ╚═════╝    ╚═╝
*/

// SendEvents accepts the mode and content of a request and puts any event(s) into it
// Note that this does not perform the request, see .Send if using a CECLient
// Note that ces[1...] are dropped unless mode is batch
func SendEvents(mapper j.CEToMap, req *fasthttp.Request, ces []j.CloudEvent, mode j.Mode) (err error) {
	if len(ces) < 1 && mode != j.ModeBatch {
		return fmt.Errorf("Could not put %d events in mode %d", len(ces), mode)
	}

	rr := ReqResFromReq(req)
	// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#13-content-modes
	switch mode {
	case j.ModeBinary:
		err := rr.CEToBinary(mapper, ces[0])
		if err != nil {
			return fmt.Errorf("Could not send binary event: %s", err.Error())
		}
		return nil
	case j.ModeStructure:
		err := rr.CEToStructureJSON(mapper, ces[0])
		if err != nil {
			return fmt.Errorf("Could not send structure event: %s", err.Error())
		}
		return nil
	case j.ModeBatch:
		err := rr.CEToBatchJSON(mapper, ces)
		if err != nil {
			return fmt.Errorf("Could not send batch events: %s", err.Error())
		}
		return nil
	default:
		err = fmt.Errorf("Unknown mode: %d", mode)
	}
	return
}

/*
  ██████╗██╗     ██╗███████╗███╗   ██╗████████╗     ██╗███╗   ██╗
 ██╔════╝██║     ██║██╔════╝████╗  ██║╚══██╔══╝     ██║████╗  ██║
 ██║     ██║     ██║█████╗  ██╔██╗ ██║   ██║        ██║██╔██╗ ██║
 ██║     ██║     ██║██╔══╝  ██║╚██╗██║   ██║        ██║██║╚██╗██║
 ╚██████╗███████╗██║███████╗██║ ╚████║   ██║        ██║██║ ╚████║
  ╚═════╝╚══════╝╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝        ╚═╝╚═╝  ╚═══╝
*/

// RecvEvents receives cloudevents from a Response in any mode
// It is used for clients reading events.
func RecvEvents(mapper j.MapToCE, res *fasthttp.Response) (ces []j.CloudEvent, mode j.Mode, err error) {
	rr := ReqResFromRes(res)

	if mode, err = rr.GetMode(); err != nil {
		err = fmt.Errorf("Could not get mode: %s", err.Error())
		return
	}

	switch mode {
	case j.ModeBinary:
		ce, err := rr.BinaryToCE(mapper)
		if err != nil {
			return ces, mode, fmt.Errorf("Could not receive binary event: %s", err.Error())
		}
		ces = append(ces, ce)
		return ces, mode, nil
	case j.ModeStructure:
		// Determine the media type with which to parse the structured event
		// Or reject anything other than JSON
		// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#3-http-message-mapping
		ct := string(res.Header.Peek("Content-Type"))
		if !strings.HasPrefix(ct, mode.ContentTypePlus("json")) {
			return ces, mode, fmt.Errorf("Unknown event content media type: %s", ct)
		}

		ce, err := rr.StructureJSONToCE(mapper)
		if err != nil {
			return ces, mode, fmt.Errorf("Could not receive structure event: %s", err.Error())
		}
		ces = append(ces, ce)
		return ces, mode, nil
	case j.ModeBatch:
		// Determine the media type with which to parse the batch structured events
		// Or reject anything other than JSON
		// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#3-http-message-mapping
		ct := string(res.Header.Peek("Content-Type"))
		if !strings.HasPrefix(ct, mode.ContentTypePlus("json")) {
			return ces, mode, fmt.Errorf("Unknown event content media type: %s", ct)
		}

		ces, err := rr.BatchJSONToCE(mapper)
		if err != nil {
			return ces, mode, fmt.Errorf("Could not receive batch events: %s", err.Error())
		}
		return ces, mode, nil
	default:
		err = fmt.Errorf("Unknown mode: %d", mode)
	}
	return

}

/*
  ██████╗ ███████╗███╗   ██╗███████╗██████╗ ██╗ ██████╗
 ██╔════╝ ██╔════╝████╗  ██║██╔════╝██╔══██╗██║██╔════╝
 ██║  ███╗█████╗  ██╔██╗ ██║█████╗  ██████╔╝██║██║
 ██║   ██║██╔══╝  ██║╚██╗██║██╔══╝  ██╔══██╗██║██║
 ╚██████╔╝███████╗██║ ╚████║███████╗██║  ██║██║╚██████╗
  ╚═════╝ ╚══════╝╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝╚═╝ ╚═════╝
*/

type req *fasthttp.Request

// IsReqRes is an explicit guarantee that the underlying type is
// consistent with a fasthttp.Request/Response type,
// such as .Header
// func(req)IsReqRes(){}

type res *fasthttp.Response

// IsReqRes is an explicit guarantee that the underlying type is
// consistent with a fasthttp.Request/Response type,
// such as .Header
// func(res)IsReqRes(){}

// ReqRes represents either a fasthttp.Request or fasthttp.Response
// Interfaces cannot be used conveniently because of property dependence
type ReqRes struct {
	r interface{} // Underlying request or response
	// AppendBody([]byte)
	// Body() []byte
	// IsReqRes()
}

// ReqResFromRes casts a Request as a ReqRes
func ReqResFromReq(req *fasthttp.Request) ReqRes {
	return ReqRes{
		r: req,
	}
}

// ReqResFromRes casts a Response as a ReqRes
func ReqResFromRes(res *fasthttp.Response) ReqRes {
	return ReqRes{
		r: res,
	}
}

// Body returns the body from a ReqRes
func (rr ReqRes) Body() (p []byte, err error) {
	switch v := rr.r.(type) {
	case *fasthttp.Request:
		p = v.Body()
	case *fasthttp.Response:
		p = v.Body()
	case fasthttp.Request:
		p = v.Body()
	case fasthttp.Response:
		p = v.Body()
	default:
		err = fmt.Errorf("Body: Invalid ReqRes type: %T", v)
	}
	return p, err
}

// AppendBody writes to the body of a ReqRes
func (rr ReqRes) AppendBody(p []byte) (err error) {
	switch v := rr.r.(type) {
	case *fasthttp.Request:
		v.AppendBody(p)
	case *fasthttp.Response:
		v.AppendBody(p)
	case fasthttp.Request:
		v.AppendBody(p)
	case fasthttp.Response:
		v.AppendBody(p)
	default:
		err = fmt.Errorf("AppendBody: Invalid ReqRes type: %T", v)
	}
	return err
}

// RRHeader represents the header expected from anything supported by ReqRes
type RRHeader interface {
	Header() []byte
	IsHTTP11() bool
	Len() int
	Peek(key string) []byte
	PeekBytes(key []byte) []byte
	Set(key, value string)
	SetBytesK(key []byte, value string)
	SetBytesKV(key, value []byte)
	SetBytesV(key string, value []byte)
	String() string
	VisitAll(f func(key, value []byte))
}

// Header gets a RRHeader from a ReqRes
func (rr ReqRes) Header() (RRHeader, error) {
	switch v := rr.r.(type) {
	case *fasthttp.Request:
		return &v.Header, nil
	case *fasthttp.Response:
		return &v.Header, nil
	case fasthttp.Request:
		return &v.Header, nil
	case fasthttp.Response:
		return &v.Header, nil
	default:
		return nil, fmt.Errorf("Header: Invalid ReqRes type: %T", v)
	}
}

// Writing CE //

// CEToBinary puts a CloudEvent into a Request or Response in Binary mode
func (rr ReqRes) CEToBinary(mapper j.CEToMap, ce j.CloudEvent) (err error) {
	head, err := rr.Header()
	if err != nil {
		return fmt.Errorf("Could not access Header: %s", err.Error())
	}

	cm := j.CEMap{}
	err = cm.FromCE(mapper, ce)
	if err != nil {
		return fmt.Errorf("Could not map event: %s", err.Error())
	}

	props := []string{
		// Required
		"id",
		"source",
		"specversion",
		"type",
		// Optional
		"dataschema",
		"subject",
		"time",
	}
	for _, p := range props {
		if cm[p] == nil {
			continue
		}
		s, ok := cm[p].(string)
		if !ok {
			return fmt.Errorf("Mapped non-string: %s", p)
		}
		head.Set(fmt.Sprintf("ce-%s", p), s)
	}
	// Optional
	datacontenttype, ok := cm["datacontenttype"].(string)
	if !ok {
		datacontenttype = DefaultContentType
	}
	head.Set("Content-Type", datacontenttype)

	// Additional - Data
	if b64, ok := cm["data_base64"].([]byte); ok && len(b64) > 0 {
		rr.AppendBody(b64)
	}

	// Additional - Extensions
	ex, err := j.GetMapExtensions(cm)
	if err != nil {
		return fmt.Errorf("Could not read Extensions: %s", err.Error())
	}

	for k, v := range ex {
		bytes, err := json.Marshal(v)
		if err != nil {
			bytes = []byte(fmt.Sprintf("%s", v))
		}
		head.SetBytesV(fmt.Sprintf("ce-%s", k), bytes)
	}
	return nil
}

// CEToStructureJSON puts a CloudEvent into a Request or Response in JSON in Structure mode
func (rr ReqRes) CEToStructureJSON(mapper j.CEToMap, ce j.CloudEvent) (err error) {
	head, err := rr.Header()
	if err != nil {
		return fmt.Errorf("Could not access Header: %s", err.Error())
	}

	cm := j.CEMap{}
	err = cm.FromCE(mapper, ce)
	if err != nil {
		return fmt.Errorf("Could not map event: %s", err.Error())
	}

	js, err := ce.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Could not marshal event: %s", err.Error())
	}

	rr.AppendBody(js)
	head.Set("Content-Type", j.ModeStructure.ContentTypePlus("json"))
	return nil
}

// CEToBatchJSON puts a CloudEvent into a Request or Response in JSON in Batch mode
func (rr ReqRes) CEToBatchJSON(mapper j.CEToMap, ces j.CloudEvents) (err error) {
	head, err := rr.Header()
	if err != nil {
		return fmt.Errorf("Could not access Header: %s", err.Error())
	}

	cms := j.CEMaps{}
	err = cms.FromCEs(mapper, ces)
	if err != nil {
		return fmt.Errorf("Could not map event: %s", err.Error())
	}

	js, err := json.Marshal(cms)
	if err != nil {
		return fmt.Errorf("Could not marshal event: %s", err.Error())
	}

	rr.AppendBody(js)
	head.Set("Content-Type", j.ModeBatch.ContentTypePlus("json"))

	return nil
}

// Reading CE //

// BinaryToCE reads a Request or Response in Binary mode into CloudEvents
func (rr ReqRes) BinaryToCE(mapper j.MapToCE) (ce j.CloudEvent, err error) {
	cm := j.CEMap{} // map[string]interface{}{}

	head, err := rr.Header()
	if err != nil {
		return ce, fmt.Errorf("Could not get Header: %s", err.Error())
	}

	// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#311-http-content-type
	dct := string(head.Peek("ce-datacontenttype"))
	if len(dct) > 0 {
		return ce, fmt.Errorf("Expected empty ce-datacontenttype to be empty, got: %s", dct)
	}

	// Required + Optional
	// Note that headers ce-data_base64 and ce-data will be dropped to prevent conflicts
	head.VisitAll(func(K, v []byte) {
		k := strings.ToLower(string(K))
		if !strings.HasPrefix(k, "ce-") {
			return
		}
		key := strings.TrimPrefix(k, "ce-")
		if key == "data" || key == "data_base64" {
			err = fmt.Errorf("Binary header forbidden: %s", key)
		}
		cm[key] = string(v)
	})
	if err != nil {
		return ce, fmt.Errorf("Could not read binary headers: %s", err.Error())
	}

	dct = string(head.Peek("Content-Type"))
	if len(dct) < 1 {
		dct = DefaultContentType
	}
	cm["datacontenttype"] = dct

	// Additional
	body, err := rr.Body()
	if err != nil {
		return ce, fmt.Errorf("Could not read body: %s", err.Error())
	}
	j.SetData(cm, body)

	ce, err = cm.ToCE(mapper)
	if err != nil {
		err = fmt.Errorf("Map error: %s", err.Error())
	}
	return ce, err
}

// StructureJSONToCE reads a Request or Response in JSON in Structure mode into CloudEvents
func (rr ReqRes) StructureJSONToCE(mapper j.MapToCE) (ce j.CloudEvent, err error) {
	body, err := rr.Body()
	if err != nil {
		return ce, fmt.Errorf("Could not read body: %s", err.Error())
	}

	cm := j.CEMap{}
	err = json.Unmarshal(body, &cm)
	if err != nil {
		err = fmt.Errorf("Could not unmarshal to map: %s", err.Error())
		return
	}

	ce, err = cm.ToCE(mapper)
	if err != nil {
		err = fmt.Errorf("Map error: %s", err.Error())
	}

	if len(ce.DataContentType) < 1 {
		ce.DataContentType = DefaultContentTypeJSON
	}

	return
}

// BatchJSONToCE reads a Request or Response in JSON in Batch mode into CloudEvents
func (rr ReqRes) BatchJSONToCE(mapper j.MapToCE) (ces j.CloudEvents, err error) {
	body, err := rr.Body()
	if err != nil {
		return ces, fmt.Errorf("Could not read body: %s", err.Error())
	}

	cms := j.CEMaps{}
	err = json.Unmarshal(body, &cms)
	if err != nil {
		err = fmt.Errorf("Could not unmarshal to map: %s", err.Error())
		return
	}

	ces, err = cms.ToCEs(mapper)
	if err != nil {
		err = fmt.Errorf("Map error: %s", err.Error())
	}

	for i := range ces {
		if len(ces[i].DataContentType) < 1 {
			ces[i].DataContentType = DefaultContentTypeJSON
		}
	}

	return
}

/*
 ██╗   ██╗████████╗██╗██╗     ██╗████████╗██╗███████╗███████╗
 ██║   ██║╚══██╔══╝██║██║     ██║╚══██╔══╝██║██╔════╝██╔════╝
 ██║   ██║   ██║   ██║██║     ██║   ██║   ██║█████╗  ███████╗
 ██║   ██║   ██║   ██║██║     ██║   ██║   ██║██╔══╝  ╚════██║
 ╚██████╔╝   ██║   ██║███████╗██║   ██║   ██║███████╗███████║
  ╚═════╝    ╚═╝   ╚═╝╚══════╝╚═╝   ╚═╝   ╚═╝╚══════╝╚══════╝
*/

// GetMode uses the Content Type header to determine the content mode of the request
// https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md#3-http-message-mapping
func (rr ReqRes) GetMode() (mode j.Mode, err error) {
	head, err := rr.Header()
	if err != nil {
		err = fmt.Errorf("Could not get Header: %s", err.Error())
		return
	}
	ct := string(head.Peek("Content-Type"))

	mode = j.ModeBinary
	if strings.HasPrefix(ct, j.ModeBatch.ContentType()) {
		mode = j.ModeBatch
	} else if strings.HasPrefix(ct, j.ModeStructure.ContentType()) {
		mode = j.ModeStructure
	}
	return
}

/*
██╗    ██╗██████╗  █████╗ ██████╗ ██████╗ ███████╗██████╗ ███████╗
██║    ██║██╔══██╗██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔════╝
██║ █╗ ██║██████╔╝███████║██████╔╝██████╔╝█████╗  ██████╔╝███████╗
██║███╗██║██╔══██╗██╔══██║██╔═══╝ ██╔═══╝ ██╔══╝  ██╔══██╗╚════██║
╚███╔███╔╝██║  ██║██║  ██║██║     ██║     ███████╗██║  ██║███████║
 ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝
Deprecation candidates...
*/

// READ REQUEST

// RequestBinaryToCE reads a CloudEvent from a given Request in Binary mode
func RequestBinaryToCE(req *fasthttp.Request) (ce j.CloudEvent, err error) {
	if ce, err = ReqResFromReq(req).BinaryToCE(j.DefaultMapToCE); err != nil {
		err = fmt.Errorf("Read Request Error: %s", err.Error())
	}
	return
}

// RequestStructureJSONToCE reads a CloudEvent from a given Request in JSON in Structured mode
func RequestStructureJSONToCE(req *fasthttp.Request) (ce j.CloudEvent, err error) {
	if ce, err = ReqResFromReq(req).StructureJSONToCE(j.DefaultMapToCE); err != nil {
		err = fmt.Errorf("Read Request Error: %s", err.Error())
	}
	return
}

// RequestBatchJSONToCE reads CloudEvents from a given Request in JSON in Batch mode
func RequestBatchJSONToCE(req *fasthttp.Request) (ces []j.CloudEvent, err error) {
	if ces, err = ReqResFromReq(req).BatchJSONToCE(j.DefaultMapToCE); err != nil {
		err = fmt.Errorf("Read Request Error: %s", err.Error())
	}
	return
}

// READ RESPONSE

// ResponseBinaryToCE reads a CloudEvent from a given Response in Binary mode
func ResponseBinaryToCE(res *fasthttp.Response) (ce j.CloudEvent, err error) {
	if ce, err = ReqResFromRes(res).BinaryToCE(j.DefaultMapToCE); err != nil {
		err = fmt.Errorf("Read Response Error: %s", err.Error())
	}
	return
}

// ResponseStructureJSONToCE reads a CloudEvent from a given Response in JSON in Structured mode
func ResponseStructureJSONToCE(res *fasthttp.Response) (ce j.CloudEvent, err error) {
	if ce, err = ReqResFromRes(res).StructureJSONToCE(j.DefaultMapToCE); err != nil {
		err = fmt.Errorf("Read Response Error: %s", err.Error())
	}
	return
}

// ResponseBatchJSONToCE reads CloudEvents from a given Response in JSON in Batch mode
func ResponseBatchJSONToCE(res *fasthttp.Response) (ces j.CloudEvents, err error) {
	if ces, err = ReqResFromRes(res).BatchJSONToCE(j.DefaultMapToCE); err != nil {
		err = fmt.Errorf("Read Response Error: %s", err.Error())
	}
	return
}

// WRITE REQUEST

// CEToRequestBinary applies a CloudEvent to a given Request in Binary mode
func CEToRequestBinary(req *fasthttp.Request, ce j.CloudEvent) (err error) {
	if err := ReqResFromReq(req).CEToBinary(j.DefaultCEToMap, ce); err != nil {
		err = fmt.Errorf("Write Request Error: %s", err.Error())
	}
	return
}

// CEToRequestStructureJSON applies a CloudEvent to a given Request in JSON in Structured mode
func CEToRequestStructureJSON(req *fasthttp.Request, ce j.CloudEvent) (err error) {
	if err := ReqResFromReq(req).CEToStructureJSON(j.DefaultCEToMap, ce); err != nil {
		err = fmt.Errorf("Write Request Error: %s", err.Error())
	}
	return
}

// CEToRequestBatchJSON applies CloudEvents to a given Request in JSON in Batch mode
func CEToRequestBatchJSON(req *fasthttp.Request, ces j.CloudEvents) (err error) {
	if err := ReqResFromReq(req).CEToBatchJSON(j.DefaultCEToMap, ces); err != nil {
		err = fmt.Errorf("Write Request Error: %s", err.Error())
	}
	return
}

// WRITE RESPONSE

// CEToResponseBinary applies a CloudEvent to a Response in Binary mode
func CEToResponseBinary(res *fasthttp.Response, ce j.CloudEvent) (err error) {
	if err := ReqResFromRes(res).CEToBinary(j.DefaultCEToMap, ce); err != nil {
		err = fmt.Errorf("Write Response Error: %s", err.Error())
	}
	return
}

// CEToResponseStructureJSON applies a CloudEvent to a Response in Structured mode with JSON content type
func CEToResponseStructureJSON(res *fasthttp.Response, ce j.CloudEvent) (err error) {
	if err := ReqResFromRes(res).CEToStructureJSON(j.DefaultCEToMap, ce); err != nil {
		err = fmt.Errorf("Write Response Error: %s", err.Error())
	}
	return
}

// CEToResponseBatchJSON applies CloudEvents to a Response in Batch mode with JSON content type
func CEToResponseBatchJSON(res *fasthttp.Response, ces j.CloudEvents) (err error) {
	if err := ReqResFromRes(res).CEToBatchJSON(j.DefaultCEToMap, ces); err != nil {
		err = fmt.Errorf("Write Response Error: %s", err.Error())
	}
	return
}

// DEPRECATING...

// CtxBinaryToCE reads a RequestCtx in Binary mode into a jsonce CloudEvent
// Deprecation candidate
func CtxBinaryToCE(ctx *fasthttp.RequestCtx) (ce j.CloudEvent, err error) {
	return RequestBinaryToCE(&ctx.Request)
}

// CtxStructureJSONToCE reads a RequestCtx in Structured mode with JSON content type into a jsonce CloudEvent
// Deprecation candidate
func CtxStructureJSONToCE(ctx *fasthttp.RequestCtx) (ce j.CloudEvent, err error) {
	return RequestStructureJSONToCE(&ctx.Request)
}

// CtxBatchJSONToCE reads a RequestCtx in batched mode with JSON content type into jsonce CloudEvents
// Deprecation candidate
func CtxBatchJSONToCE(ctx *fasthttp.RequestCtx) (ces []j.CloudEvent, err error) {
	return RequestBatchJSONToCE(&ctx.Request)
}

// CEToCtxBinary applies a jsonce CloudEvent to a RequestCtx in Binary mode
// Deprecation candidate
func CEToCtxBinary(ctx *fasthttp.RequestCtx, ce j.CloudEvent) (err error) {
	return CEToResponseBinary(&ctx.Response, ce)
}

// CEToCtxStructureJSON applies a jsonce CloudEvent to a RequestCtx in Structured mode with JSON content type
// Deprecation candidate
func CEToCtxStructureJSON(ctx *fasthttp.RequestCtx, ce j.CloudEvent) (err error) {
	return CEToResponseStructureJSON(&ctx.Response, ce)
}

// CEToCtxBatchJSON applies a jsonce CloudEvent to a RequestCtx in batched mode with JSON content type
// Deprecation candidate
func CEToCtxBatchJSON(ctx *fasthttp.RequestCtx, ces []j.CloudEvent) (err error) {
	return CEToResponseBatchJSON(&ctx.Response, ces)
}
