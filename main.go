package main

import (
	"os"

	"github.com/urfave/cli"
	"github.com/roblaszczak/simple-kafka"
	"strings"
	"fmt"
	"log"
	"errors"
	"encoding/json"
	"bytes"
	"runtime"
	"github.com/fatih/color"
)

const (
	defaultKafkaPort = "9092"
)

var (
	logger       = log.New(os.Stdout, "[clikafka] ", log.LstdFlags|log.Lshortfile)
	NoBrokersErr = errors.New("you must provide at least one broker using --brokers flag")

	// color functions
	cyan = color.New(color.FgCyan).SprintFunc()
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "brokers",
			Usage: "comma separated brokers list, port is optional, for example --brokers 127.0.0.1:9092,127.0.0.2",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "topics",
			Aliases: []string{"t"},
			Usage:   "lists topics",
			Action: func(c *cli.Context) error {
				client, err := createClient(c)
				if err != nil {
					return err
				}

				topics, err := client.Topics()
				if err != nil {
					return err
				}
				fmt.Println(topics)

				return nil
			},
		},
		{
			Name:    "consume",
			Aliases: []string{"c"},
			Usage:   "consume messages from topic",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "from-beginning",
					Usage: "if flag is provided all messages will be shown",
				},
				cli.BoolFlag{
					Name:  "disable-json-pretty",
					Usage: "this flag disables JSON prettifying",
				},
			},
			Action: func(c *cli.Context) error {
				client, err := createClient(c)
				if err != nil {
					return err
				}

				var offset int64
				if c.Bool("from-beginning") {
					offset = kafka.OffsetEarliest
				} else {
					offset = kafka.OffsetLastest
				}

				topic := c.Args().First()
				messages, err := client.Consume(topic, offset)
				if err != nil {
					return err
				}

				workersCount := runtime.NumCPU()
				logger.Printf("creating %d printMessageWorkers", workersCount)
				workersInput := make([]chan kafka.Message, workersCount)

				for i := 0; i < workersCount; i++ {
					workersInput[i] = make(chan kafka.Message)
					go printMessageWorker(i, workersInput[i], c.Bool("disable-json-pretty"))
				}

				for {
					for _, c := range workersInput {
						val := <-messages
						c <- val
					}
				}

				return nil
			},
		},
	}

	app.Run(os.Args)
}

func printMessageWorker(num int, messages <-chan kafka.Message, disableJsonPretty bool) {
	for message := range messages {
		rawJson := message.Value

		var parsed string
		var prettyErr error

		if !disableJsonPretty {
			var pretty []byte
			if pretty, prettyErr = prettyJson(rawJson); prettyErr == nil {
				parsed = string(pretty)
			}
		}
		if prettyErr != nil || disableJsonPretty {
			parsed = string(rawJson)
		}

		fmt.Printf(
			"Recieved message - topic: %s, partition: %s, offset: %s, value: \n%s\n",
			cyan(message.Topic),
			cyan(message.Partition),
			cyan(message.Offset),
			parsed,
		)
	}
}

func prettyJson(rawJson []byte) ([]byte, error) {
	var prettyJSONBuf bytes.Buffer
	err := json.Indent(&prettyJSONBuf, rawJson, "", "\t")
	if err != nil {
		return []byte{}, err
	}

	return prettyJSONBuf.Bytes(), nil
}

func createClient(c *cli.Context) (kafka.Client, error) {
	brokersStr := c.GlobalString("brokers")
	if len(brokersStr) == 0 {
		return nil, NoBrokersErr
	}
	brokers := strings.Split(brokersStr, ",")

	for num, broker := range brokers {
		brokerParts := strings.Split(broker, ":")
		if len(brokerParts) == 1 {
			brokerParts = append(brokerParts, defaultKafkaPort)
		}

		brokers[num] = strings.Join(brokerParts, ":")
	}

	logger.Println("creating kafka client for brokers", brokers)

	return kafka.NewClient(brokers)
}
