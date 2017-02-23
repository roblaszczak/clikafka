# CLIkafka

Simple CLI tool for Kafka written in GO (golang).
For this moment only consumer is supported.

## Installation

Just run

    go get github.com/roblaszczak/clikafka


## Usage

For available options run

    clikafka --help
    
    
For every command you must provide at least one broker. 
You can provide more than one broker If you don't provide port, `9092` will be used.


    clikafka --broker=kafka1.example.com --broker=kafka2.example.com:4292 consume my_pro_topic

### Consume

Simplest consumer usage:

    clikafka --broker=my-kafka-server.com consume my_pro_topic

By default only newest messages are consumed. To consume older messages you should run

    clikafka --broker=my-kafka-server.com:9092 consume --from-beginning my_pro_topic

### List topics

To list topics run

    clikafka --broker=my-kafka-server.com:9092 topics

## Credits

Made without love by Robert Laszczak </3

## License

MIT