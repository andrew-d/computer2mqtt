package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/shirou/gopsutil/v3/load"
	flag "github.com/spf13/pflag"
)

// Connect to the broker and publish a message periodically
func main() {
	// TODO: config
	var (
		keepAlive         = flag.Duration("keep-alive", 30*time.Second, "time between keep-alive packets")
		connectRetryDelay = flag.Duration("connect-retry-delay", 10*time.Second, "time between connection attempts")
		clientID          = flag.String("client-id", "computer2mqtt", "client ID to use when connecting to server")

		prefix   = flag.String("prefix", "", "MQTT topic prefix")
		qos      = flag.Uint8("qos", 0, "QoS byte")
		interval = flag.Duration("interval", 5*time.Second, "reporting interval")

		debug         = flag.Bool("debug", false, "enable debug")
		printMessages = flag.Bool("print-messages", false, "print messages to stdout")
	)
	flag.Parse()

	log.SetOutput(os.Stderr)

	broker := flag.Arg(0)
	if broker == "" {
		fmt.Fprintf(os.Stderr, "usage: %s [options] BROKER\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.Printf("connecting to broker: %s", broker)
	u, err := url.Parse(broker)
	if err != nil {
		panic(err)
	}

	cliCfg := autopaho.ClientConfig{
		BrokerUrls:        []*url.URL{u},
		KeepAlive:         uint16(*keepAlive / time.Second),
		ConnectRetryDelay: *connectRetryDelay,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			log.Println("mqtt connection up")
		},
		OnConnectError: func(err error) {
			log.Printf("error whilst attempting connection: %s\n", err)
		},
		Debug: paho.NOOPLogger{},
		ClientConfig: paho.ClientConfig{
			ClientID: *clientID,
			OnClientError: func(err error) {
				log.Printf("server requested disconnect: %s\n", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					log.Printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					log.Printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	if *debug {
		cliCfg.Debug = logger{prefix: "autoPaho"}
		cliCfg.PahoDebug = logger{prefix: "paho"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the broker - this will return immediately after initiating the connection process
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	// Start off a goRoutine that publishes messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// AwaitConnection will return immediately if connection is up; adding this call stops publication whilst
			// connection is unavailable.
			err = cm.AwaitConnection(ctx)
			if err != nil { // Should only happen when context is cancelled
				log.Printf("publisher done (AwaitConnection: %s)\n", err)
				return
			}

			loadAvg, err := load.Avg()
			if err == nil {
				msg, err := json.Marshal(map[string]interface{}{
					"load1":  loadAvg.Load1,
					"load5":  loadAvg.Load5,
					"load15": loadAvg.Load15,
				})
				if err != nil {
					panic(err)
				}

				// Publish will block so we run it in a goRoutine
				go func(msg []byte) {
					pr, err := cm.Publish(ctx, &paho.Publish{
						QoS:     *qos,
						Topic:   *prefix + "load",
						Payload: msg,
					})
					if err != nil {
						log.Printf("error publishing: %s\n", err)
					} else if pr != nil && pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers
						log.Printf("reason code %d received\n", pr.ReasonCode)
					} else if *printMessages {
						fmt.Printf("%s\n", msg)
					}
				}(msg)
			}

			select {
			case <-time.After(*interval):
			case <-ctx.Done():
				log.Println("publisher done")
				return
			}
		}
	}()

	// Wait for a signal before exiting
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	log.Println("signal caught - exiting")
	cancel()

	wg.Wait()
	log.Println("shutdown complete")
}

// logger implements the paho.Logger interface
type logger struct {
	prefix string
}

// Println is the library provided NOOPLogger's
// implementation of the required interface function()
func (l logger) Println(v ...interface{}) {
	log.Println(append([]interface{}{l.prefix + ":"}, v...)...)
}

// Printf is the library provided NOOPLogger's
// implementation of the required interface function(){}
func (l logger) Printf(format string, v ...interface{}) {
	if len(format) > 0 && format[len(format)-1] != '\n' {
		format = format + "\n" // some log calls in paho do not add \n
	}
	log.Printf(l.prefix+":"+format, v...)
}
