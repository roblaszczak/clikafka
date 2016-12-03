# CLIkafka

Simple CLI tool for Kafka written in GO (golang).
For this moment only consumer is supported.

## Installation

Just run

    go get github.com/roblaszczak/clikafka


## Usage

Simplest consumer usage:

    clikafka my_pro_topic --broker=my-kafka-server.com:9092

For available options run

    clikafka --help

## Credits

Made without love by Robert Laszczak </3

## License

MIT