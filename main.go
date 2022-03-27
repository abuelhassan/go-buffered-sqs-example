package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/abuelhassan/go-buffered-sqs-example/queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var (
	queueURL string
)

func generator(ctx context.Context, wg *sync.WaitGroup, q queue.Queue) {
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			time.Sleep(time.Duration(50+rand.Int()%151) * time.Millisecond) // add some jitter value (from 50ms to 200ms)

			for i := 0; i < 1+rand.Int()%15; i++ { // random value from 1 to 15
				body := fmt.Sprintf("%d_%s", i, time.Now().UTC().Format(time.RFC3339Nano))
				q.SendMessage(body)
			}
		case <-ctx.Done():
			return
		}
	}
}

func poller(ctx context.Context, wg *sync.WaitGroup, q queue.Queue, ch chan<- queue.ReceiveOutput) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Printf("receiving...")
			msgs, err := q.ReceiveMessages(ctx)
			if err != nil {
				log.Printf("unable to receive messages, %v\n", err) // report error
				continue
			}
			log.Printf("%d messages received at %s\n", len(msgs), time.Now().UTC().Format(time.RFC3339Nano))
			for _, msg := range msgs {
				ch <- msg
			}
		}
	}
}

func processor(wg *sync.WaitGroup, q queue.Queue, ch <-chan queue.ReceiveOutput) {
	defer wg.Done()

	for msg := range ch {
		time.Sleep(500 * time.Millisecond) // for debugging.
		q.DeleteMessage(msg.ReceiptHandle)
		log.Printf("processed %s\n", msg.Body)
	}
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	ch := make(chan queue.ReceiveOutput, 50)
	q := queue.GetBufferedInstance(queueURL)

	// run generator
	wg.Add(1)
	go generator(ctx, &wg, q)

	// run poller
	wg.Add(1)
	go poller(ctx, &wg, q, ch)

	// run processor
	wg.Add(1)
	go processor(&wg, q, ch)

	go func() {
		<-c
		cancel()

		log.Println("Shutting Down...")
		time.Sleep(20 * time.Second) // Some time for messages being processed. It can be smarter.
		close(ch)
	}()

	wg.Wait()
}

// Normally you don't create infra this way.
func init() {
	const (
		awsRegion   = "us-east-1"
		awsEndpoint = "http://localhost:4566" // localstack
		queueName   = "test-queue"
	)

	resolver := func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           awsEndpoint,
			SigningRegion: awsRegion,
		}, nil
	}

	cfg, err := awsConfig.LoadDefaultConfig(
		context.Background(),
		awsConfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(resolver)),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	q, err := sqs.NewFromConfig(cfg).CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	if err != nil {
		log.Fatalf("unable to create queue, %v", err)
	}

	queueURL = *q.QueueUrl
}
