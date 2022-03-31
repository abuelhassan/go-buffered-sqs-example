package queue

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var (
	sqsClient *sqs.Client
)

type Queue interface {
	SendMessage(body string)
	DeleteMessage(receiptHandle string)
	ReceiveMessages(ctx context.Context) ([]ReceiveOutput, error)
}

type ReceiveOutput struct {
	Body          string
	ReceiptHandle string
}

type bufferedQueue struct {
	url           string
	sendBufferDur time.Duration
	delBufferDur  time.Duration
	sendCh        chan string // channel of message bodies
	delCh         chan string // channel of receipt handles
}

func GetBufferedInstance(url string) Queue {
	const bufferSize = 10

	q := bufferedQueue{
		url:           url,
		sendBufferDur: 10 * time.Second,
		delBufferDur:  5 * time.Second,
		sendCh:        make(chan string, 50),
		delCh:         make(chan string, 50),
	}

	go func() {
		ticker := time.NewTicker(q.sendBufferDur)
		buffer := make([]string, 0, bufferSize)
		for {
			select {
			case msg := <-q.sendCh:
				buffer = append(buffer, msg)
				if len(buffer) < bufferSize {
					continue
				}
			case <-ticker.C:
				if len(buffer) == 0 {
					continue
				}
			}
			entries := make([]types.SendMessageBatchRequestEntry, len(buffer))
			for i, msg := range buffer {
				entries[i] = types.SendMessageBatchRequestEntry{
					Id:          aws.String(strconv.Itoa(i)),
					MessageBody: aws.String(msg),
				}
			}
			log.Printf("Sending: %d", len(buffer))
			_, err := sqsClient.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{
				Entries:  entries,
				QueueUrl: &url,
			})
			if err != nil {
				log.Printf("unable to send messages, %v\n", err) // report error
				continue
			}
			buffer = make([]string, 0, bufferSize)
		}
	}()

	go func() {
		ticker := time.NewTicker(q.delBufferDur)
		buffer := make([]string, 0, bufferSize)
		for {
			select {
			case msg := <-q.delCh:
				buffer = append(buffer, msg)
				if len(buffer) < bufferSize {
					continue
				}
			case <-ticker.C:
				if len(buffer) == 0 {
					continue
				}
			}
			entries := make([]types.DeleteMessageBatchRequestEntry, len(buffer))
			for i, msg := range buffer {
				entries[i] = types.DeleteMessageBatchRequestEntry{
					Id:            aws.String(strconv.Itoa(i)),
					ReceiptHandle: aws.String(msg),
				}
			}
			log.Printf("Deleting: %d", len(buffer))
			_, err := sqsClient.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
				Entries:  entries,
				QueueUrl: &url,
			})
			if err != nil {
				log.Printf("unable to delete messages, %v\n", err) // report error
				continue
			}
			buffer = make([]string, 0, bufferSize)
		}
	}()

	return &q
}

func (q bufferedQueue) SendMessage(body string) {
	q.sendCh <- body
}

func (q bufferedQueue) DeleteMessage(receiptHandle string) {
	q.delCh <- receiptHandle
}

func (q bufferedQueue) ReceiveMessages(ctx context.Context) ([]ReceiveOutput, error) {
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &q.url,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20, // long poll for 20 seconds.
	})
	if err != nil {
		return nil, err
	}
	var result []ReceiveOutput
	for _, msg := range out.Messages {
		result = append(result, ReceiveOutput{Body: *msg.Body, ReceiptHandle: *msg.ReceiptHandle})
	}
	return result, nil
}

func init() {
	const (
		awsRegion   = "us-east-1"
		awsEndpoint = "http://localhost:4566" // localstack
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

	sqsClient = sqs.NewFromConfig(cfg)
}
