package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaswanth05rongali/pub-sub/pub"
	"github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	logger       = log.With().Str("pkg", "main").Logger()
	p            *kafka.Producer
	prevTime     = time.Now()
	clientStatus = true
)

type data struct {
	Requestid     string `json:"request_id"`
	Topicname     string `json:"topic_name"`
	Messagebody   string `json:"message_body"`
	Transactionid string `json:"transaction_id"`
	Email         string `json:"email"`
	Phone         string `json:"phone"`
	Customerid    string `json:"customer_id"`
	Key           string `json:"key"`
	Retry         int    `json:"retry"`
}
type intRange struct {
	min, max int
}

func (ir *intRange) nextRandom(r *rand.Rand) int {
	return r.Intn(ir.max-ir.min+1) + ir.min
}
func main() {
	r := rand.New(rand.NewSource(55))
	ir := intRange{1, 20}
	go func() {
		for {
			time.Sleep(time.Duration(ir.nextRandom(r)) * time.Second)
			clientStatus = !clientStatus
		}
	}()

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				topic := topics

				if topic[0] == "retry_5m_topic" {
					time.Sleep(5 * time.Second)
				} else if topic[0] == "retry_30m_topic" {
					time.Sleep(10 * time.Second)
				} else if topic[0] == "retry_1hr_topic" {
					time.Sleep(15 * time.Second)
				}
				err := processMessage(e.Value, topic[0])

				temp := data{}
				json.Unmarshal(e.Value, &temp)

				if err != nil {
					if topic[0] == "foo" {
						publishTo(e.Value, "retry_5m_topic", "192.168.99.100:19092")
					} else if topic[0] == "retry_5m_topic" && temp.Retry != 2 {
						publishTo(e.Value, "retry_5m_topic", "192.168.99.100:19092")
					} else if topic[0] == "retry_5m_topic" && temp.Retry == 2 {
						publishTo(e.Value, "retry_30m_topic", "192.168.99.100:19092")
					} else if topic[0] == "retry_30m_topic" && temp.Retry != 5 {
						publishTo(e.Value, "retry_30m_topic", "192.168.99.100:19092")
					} else if topic[0] == "retry_30m_topic" && temp.Retry == 5 {
						publishTo(e.Value, "retry_1hr_topic", "192.168.99.100:19092")
					} else if topic[0] == "retry_1hr_topic" && temp.Retry != 7 {
						publishTo(e.Value, "retry_1hr_topic", "192.168.99.100:19092")
					} else if topic[0] == "retry_1hr_topic" && temp.Retry == 7 {
						publishTo(e.Value, "failed_topic", "192.168.99.100:19092")
					}
				}

				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}

func processMessage(message []byte, topic string) error {
	var ERROR error
	if clientStatus {
		fmt.Printf("\n SUCCESS SENDING : %s\n", message)
		ERROR = nil
	} else {
		fmt.Printf("\n FAILED SENDING : %s\n", message)
		ERROR = errors.New(" ")
	}
	return ERROR
}

func publishTo(message []byte, topic, kafkaBrokerURL string) {
	var err error
	p, err = pub.Producer(kafkaBrokerURL)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)
	defer p.Close()

	parent := context.Background()
	defer parent.Done()

	deliveryChan := make(chan kafka.Event)

	form := data{}
	json.Unmarshal(message, &form)
	form.Retry++
	message, _ = json.MarshalIndent(form, " ", " ")
	value := string(message)
	kafkaTopic := topic
	var kafkaMessage kafka.Message

	kafkaMessage = kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}

	err = p.Produce(&kafkaMessage, deliveryChan)
	if err != nil {
		fmt.Println("error while push message into kafka:\n", err.Error())
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(deliveryChan)
}
