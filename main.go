package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	flag "github.com/spf13/pflag"
)

// Connect to the broker and publish a message periodically
func main() {
	// TODO: config
	var (
		keepAlive         = flag.Duration("keep-alive", 30*time.Second, "time between keep-alive packets")
		connectRetryDelay = flag.Duration("connect-retry-delay", 10*time.Second, "time between connection attempts")
		clientID          = flag.String("client-id", "computer2mqtt", "client ID to use when connecting to server")

		prefix = flag.String("prefix", "computer2mqtt/", "MQTT topic prefix")
		//qos      = flag.Uint8("qos", 0, "QoS byte")
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
			log.Printf("error while attempting connection: %s\n", err)
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
	load := &periodicPublisher{
		interval:      *interval,
		bufferSize:    10,
		wg:            &wg,
		cm:            cm,
		topic:         *prefix + "load",
		printMessages: *printMessages,
		cb: func(ctx context.Context) ([]byte, error) {
			loadAvg, err := load.AvgWithContext(ctx)
			if err != nil {
				return nil, fmt.Errorf("error fetching load: %w", err)
			}

			msg, err := json.Marshal(map[string]interface{}{
				"load1":  loadAvg.Load1,
				"load5":  loadAvg.Load5,
				"load15": loadAvg.Load15,
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling JSON: %w", err)
			}

			return msg, nil
		},
	}
	cpu := &periodicPublisher{
		interval:      *interval,
		bufferSize:    10,
		wg:            &wg,
		cm:            cm,
		topic:         *prefix + "cpu",
		printMessages: *printMessages,
		cb: func(ctx context.Context) ([]byte, error) {
			numPhys, err := cpu.Counts(false)
			if err != nil {
				return nil, fmt.Errorf("error fetching CPU info: %w", err)
			}
			numLogical, err := cpu.Counts(true)
			if err != nil {
				return nil, fmt.Errorf("error fetching CPU info: %w", err)
			}

			msg, err := json.Marshal(map[string]interface{}{
				"num_physical": numPhys,
				"num_logical":  numLogical,
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling JSON: %w", err)
			}

			return msg, nil
		},
	}
	mem := &periodicPublisher{
		interval:      *interval,
		bufferSize:    10,
		wg:            &wg,
		cm:            cm,
		topic:         *prefix + "mem",
		printMessages: *printMessages,
		cb: func(ctx context.Context) ([]byte, error) {
			memInfo, err := mem.VirtualMemoryWithContext(ctx)
			if err != nil {
				return nil, fmt.Errorf("error fetching memory info: %w", err)
			}

			msg, err := json.Marshal(map[string]interface{}{
				"total":        memInfo.Total,
				"available":    memInfo.Available,
				"used":         memInfo.Used,
				"used_percent": memInfo.UsedPercent,
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling JSON: %w", err)
			}

			return msg, nil
		},
	}
	times := &periodicPublisher{
		interval:      *interval,
		bufferSize:    10,
		wg:            &wg,
		cm:            cm,
		topic:         *prefix + "times",
		printMessages: *printMessages,
		cb: func(ctx context.Context) ([]byte, error) {
			// TODO: not linux-specific
			stat, err := ioutil.ReadFile("/proc/stat")
			if err != nil {
				return nil, fmt.Errorf("error reading /proc/stat: %w", err)
			}

			msg := make(map[string]interface{})

			lines := strings.Split(string(stat), "\n")
			for _, line := range lines {
				fields := strings.Fields(line)
				if len(fields) == 0 {
					continue
				}

				switch fields[0] {
				case "btime":
					if val, err := strconv.Atoi(fields[1]); err == nil {
						msg["boot_time"] = val
					}
				}
			}

			msgb, err := json.Marshal(msg)
			if err != nil {
				return nil, fmt.Errorf("error marshaling JSON: %w", err)
			}
			return msgb, nil
		},
	}

	cpu.Run(ctx)
	load.Run(ctx)
	mem.Run(ctx)
	times.Run(ctx)

	// Wait for a signal before exiting
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	log.Println("signal caught")
	cancel()

	wg.Wait()
	log.Println("shutdown complete")
}

type periodicPublisher struct {
	cb            func(context.Context) ([]byte, error)
	cm            *autopaho.ConnectionManager
	topic         string
	interval      time.Duration
	bufferSize    int
	printMessages bool
	wg            *sync.WaitGroup
}

func (p *periodicPublisher) Run(ctx context.Context) {
	msgs := make(chan []byte, p.bufferSize)

	p.wg.Add(2)
	go p.sendLoop(ctx, msgs)
	go p.publishLoop(ctx, msgs)
}

func (p *periodicPublisher) sendLoop(ctx context.Context, publish chan []byte) {
	defer p.wg.Done()
	for msg := range publish {
		msg := msg // capture loop variable

		// AwaitConnection will return immediately if connection is up;
		// adding this call stops publication while the connection is
		// unavailable.
		err := p.cm.AwaitConnection(ctx)
		if err != nil {
			return
		}

		pr, err := p.cm.Publish(ctx, &paho.Publish{
			Topic:   p.topic,
			Payload: msg,
		})
		if err != nil {
			log.Printf("error publishing: %s\n", err)
		} else if pr != nil && pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers
			log.Printf("reason code %d received\n", pr.ReasonCode)
		} else if p.printMessages {
			fmt.Printf("%s\n", msg)
		}
	}
}

func (p *periodicPublisher) publishLoop(ctx context.Context, publish chan []byte) {
	defer p.wg.Done()
	defer close(publish)
	defer log.Printf("publisher %q done", p.topic)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		msg, err := p.cb(ctx)
		if err == nil {
			publish <- msg
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}

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
