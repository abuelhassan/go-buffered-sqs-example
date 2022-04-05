# go-buffered-sqs-example

## Why?

[To reduce costs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/reducing-costs.html)

## How to Run?

### Prerequisites

Make sure you have installed:

- Docker
- Go >=1.18

### Run localstack

```shell
docker-compose up
```

### Run the project

```shell
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

go run main.go
```
