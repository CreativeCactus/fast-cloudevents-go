package fastce

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	jsonce "github.com/creativecactus/fast-cloudevents-go/jsonce"
)

var target string

func TestMain(m *testing.M) {
	// Init
	server, shutdownErr, addr, err := ExampleServer("0.0.0.0:0", ExampleHandler)
	if err != nil {
		log.Fatalf("Server Init Error: %s", err)
	}

	target = addr

	// Run Tests
	result := m.Run()

	// Shutdown
	server.Shutdown()
	err = waitForErr(shutdownErr, 5*time.Second)
	if err != nil {
		log.Fatalf("Server Error: %s", err)
	}
	os.Exit(result)
}

func ClientTester(cec CEClient, ces []jsonce.CloudEvent, mode jsonce.Mode, count uint) (res []jsonce.CloudEvent, err error) {
	err = cec.SendEvents(jsonce.DefaultCEToMap, ces, mode)
	if err != nil {
		err = fmt.Errorf("Example failed to Send: %s", err.Error())
		return
	}
	err = cec.Send()
	if err != nil {
		err = fmt.Errorf("Example failed to transfer: %s", err.Error())
		return
	}

	res, rmode, err := cec.RecvEvents(jsonce.DefaultMapToCE)
	if err != nil {
		err = fmt.Errorf("Example failed to Recv: %s", err.Error())
		return
	}
	if uint(len(res)) != count {
		err = fmt.Errorf("Example: returned %d events instead of %d", len(res), count)
		return
	}
	if rmode != mode {
		err = fmt.Errorf("Example: returned mode %d instead of %d", rmode, mode)
		return
	}
	return
}
func TestBinary(t *testing.T) {
	count := uint(5)
	mode := jsonce.ModeBinary
	cec, err := NewCEClient("PUT", target)
	if err != nil {
		err = fmt.Errorf("Example failed to Init: %s", err.Error())
		t.Fatalf("TestBinary: %s", err.Error())
		return
	}
	defer cec.Release()

	ces := jsonce.GenerateValidEvents(count)

	res, err := ClientTester(cec, ces, mode, 1)
	if err != nil {
		t.Fatalf("TestBinary: %s", err.Error())
	}

	// Check that nano time is preserved
	for i, re := range res {
		ce := ces[i]
		if ce.Time.Format(time.RFC3339Nano) != re.Time.Format(time.RFC3339Nano) {
			t.Fatalf("Time in event %d differs in response:\n\tRequest: %s\n\tResponse: %s", i, ce.Time.Format(time.RFC3339Nano), re.Time.Format(time.RFC3339Nano))
		}
		t.Logf("CE.Time matches:\n\tRequest: %s\n\tResponse: %s", ce.Time.Format(time.RFC3339Nano), re.Time.Format(time.RFC3339Nano))
	}
}
func TestStructure(t *testing.T) {
	count := uint(5)
	mode := jsonce.ModeStructure
	cec, err := NewCEClient("PUT", target)
	if err != nil {
		err = fmt.Errorf("Example failed to Init: %s", err.Error())
		t.Fatalf("TestStructure: %s", err.Error())
		return
	}
	defer cec.Release()

	ces := jsonce.GenerateValidEvents(count)

	res, err := ClientTester(cec, ces, mode, 1)
	if err != nil {
		t.Fatalf("TestStructure: %s", err.Error())
	}

	// Check that nano time is preserved
	for i, re := range res {
		ce := ces[i]
		if ce.Time.Format(time.RFC3339Nano) != re.Time.Format(time.RFC3339Nano) {
			t.Fatalf("Time in event %d differs in response:\n\tRequest: %s\n\tResponse: %s", i, ce.Time.Format(time.RFC3339Nano), re.Time.Format(time.RFC3339Nano))
		}
		t.Logf("CE.Time matches:\n\tRequest: %s\n\tResponse: %s", ce.Time.Format(time.RFC3339Nano), re.Time.Format(time.RFC3339Nano))
	}
}
func TestBatch(t *testing.T) {
	count := uint(5)
	mode := jsonce.ModeBatch
	cec, err := NewCEClient("PUT", target)
	if err != nil {
		err = fmt.Errorf("Example failed to Init: %s", err.Error())
		t.Fatalf("TestBatch: %s", err.Error())
		return
	}
	defer cec.Release()

	ces := jsonce.GenerateValidEvents(count)

	res, err := ClientTester(cec, ces, mode, count)
	if err != nil {
		t.Fatalf("TestBatch: %s", err.Error())
	}

	if len(res) != len(ces) {
		t.Fatalf("Got %d events back but expected %d", len(res), len(ces))
	}
	t.Logf("Got %d events back as expected", len(res))

	{ // Check that nano time is preserved
		for i, re := range res {
			ce := ces[i]
			if ce.Time.Format(time.RFC3339Nano) != re.Time.Format(time.RFC3339Nano) {
				t.Fatalf("Time in event %d differs in response:\n\tRequest: %s\n\tResponse: %s", i, ce.Time.Format(time.RFC3339Nano), re.Time.Format(time.RFC3339Nano))
			}
			t.Logf("CE.Time matches:\n\tRequest: %s\n\tResponse: %s", ce.Time.Format(time.RFC3339Nano), re.Time.Format(time.RFC3339Nano))
		}
	}

	{ // Check that DataContentType is set
		for i, re := range res {
			ce := ces[i]
			if re.DataContentType != DefaultContentTypeJSON {
				t.Fatalf("DataContentType in event %d differs from expected: %s\n\tResponse: %s", i, DefaultContentTypeJSON, re.DataContentType)
			}
			t.Logf("CE.DataContentType matches:\n\tRequest: %s\n\tDefault: %s\n\tResponse: %s", ce.DataContentType, DefaultContentTypeJSON, re.DataContentType)
		}
	}

}

func TestCEClientCEServer(t *testing.T) {
	count := uint(4)
	ces := jsonce.GenerateValidEvents(count)
	mode := jsonce.ModeBatch
	t.Logf("Generated %d events to send in mode %d", len(ces), mode)

	ces, err := ExampleCEClientCEServer(ces, mode)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}

	for i, ce := range ces {
		warns, err := ce.Valid()
		if err != nil {
			t.Fatalf("Event %d validation error: %s", i, err.Error())
		}
		for _, w := range warns {
			t.Logf("	Event %d validation warning: %s", i, w.Error())
		}
	}

	t.Logf("Received: %d/%d valid events, the first has Source:%s\n", len(ces), count, ces[0].Source)
}

/*
 ██╗   ██╗████████╗██╗██╗     ███████╗
 ██║   ██║╚══██╔══╝██║██║     ██╔════╝
 ██║   ██║   ██║   ██║██║     ███████╗
 ██║   ██║   ██║   ██║██║     ╚════██║
 ╚██████╔╝   ██║   ██║███████╗███████║
  ╚═════╝    ╚═╝   ╚═╝╚══════╝╚══════╝
*/

func waitForErr(c <-chan error, t time.Duration) error {
	select {
	case err := <-c:
		return err
	case <-time.After(t):
		return nil
	}
}
