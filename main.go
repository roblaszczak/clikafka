package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/signal"
)

var (
	// CLI arguments
	brokersTCP    = kingpin.Flag("broker", "Broker IP address").Default("127.0.0.1:9092").TCPList()
	topic         = kingpin.Arg("topic", "Topic to consume.").Required().String()
	fromBeginning = kingpin.Flag("from-beginning", "If provided all messages from beginning will be consumed").Bool()
	group         = kingpin.Flag("group", "Consumer group").Default("clikafka").String()
	verbose       = kingpin.Flag("verbose", "Debug mode").Short('v').Bool()
	noColor       = kingpin.Flag("no-color", "Disable colored output").Bool()
	noPrettyJSON = kingpin.Flag("no-pretty-json", "Disable json prettyfication").Bool()

	logger *log.Logger

	// color functions
	cyan = color.New(color.FgCyan).SprintFunc()
)

func setUpLogger() {
	loggerFlags := log.LstdFlags
	if *verbose {
		loggerFlags |= log.Llongfile
	}

	logger = log.New(os.Stdout, "[clikafka] ", loggerFlags)
	sarama.Logger = logger
}

func createConsumer() *sarama.Consumer {
	brokers := []string{}
	for _, brokerTCP := range *brokersTCP {
		brokers = append(brokers, brokerTCP.String())
	}

	config := sarama.NewConfig()
	config.ClientID = *group

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	return &consumer
}

func printMsg(msg *sarama.ConsumerMessage) {
	var outValue []byte

	if !*noPrettyJSON {
		var buf bytes.Buffer
		if err := json.Indent(&buf, msg.Value, "", "\t"); err == nil {
			outValue = buf.Bytes()
			outValue = append([]byte{'\n'}, outValue...)
		}
	}
	if outValue == nil {
		outValue = msg.Value
	}

	fmt.Printf(
		"Recieved message - topic: %s, offset: %s, timestamp: %s, value: %s\n",
		cyan(*topic),
		cyan(msg.Offset),
		cyan(msg.Timestamp),
		cyan(string(outValue)),
	)
}

func consumeTopic(consumer sarama.Consumer) {
	var offset int64
	if *fromBeginning {
		offset = sarama.OffsetOldest
	} else {
		offset = sarama.OffsetNewest
	}

	partitionConsumer, err := consumer.ConsumePartition(*topic, 0, offset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			logger.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			printMsg(msg)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	logger.Printf("Consumed: %d\n", consumed)
}

func main() {
	kingpin.Version("0.1.0")
	kingpin.Parse()
	setUpLogger()

	if *noColor {
		color.NoColor = true
	}

	consumer := *createConsumer()

	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Fatalln(err)
		}
	}()

	if *verbose {
		topics, err := consumer.Topics()
		if err != nil {
			panic(err)
		}
		logger.Printf("Topics: %s", topics)
	}

	consumeTopic(consumer)
}
