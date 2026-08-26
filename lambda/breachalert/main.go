// Command breachalert is an AWS Lambda function triggered by an Amazon MSK
// event source mapping on the breaches topic. It pages operators through SNS
// and delivers signed webhooks to subscribed partners.
//
// Delivery is at-least-once: if any partner fails transiently the function
// returns an error and Lambda retries the batch (bounded by the event source
// mapping's retry policy, with failed batches sent to an SQS destination).
// Every webhook carries a stable X-Coldchain-Event-Id for deduplication.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/guangjiandong/cold-chain-monitor/internal/alert"
	"github.com/guangjiandong/cold-chain-monitor/internal/platform"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
	"github.com/guangjiandong/cold-chain-monitor/internal/webhook"
)

func main() {
	router, err := newRouter(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "breachalert init:", err)
		os.Exit(1)
	}
	lambda.Start(func(ctx context.Context, ev events.KafkaEvent) error {
		var values [][]byte
		for _, recs := range ev.Records {
			for _, r := range recs {
				v, err := base64.StdEncoding.DecodeString(r.Value)
				if err != nil {
					router.Log.Error("bad base64 record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
					continue
				}
				values = append(values, v)
			}
		}
		return router.Route(ctx, values)
	})
}

// newRouter builds dependencies once per execution environment (cold start)
// so connections are reused across invocations.
func newRouter(ctx context.Context) (*alert.Router, error) {
	log := platform.NewLogger("breach-alert")

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	topicARN := os.Getenv("ALERT_TOPIC_ARN")
	if topicARN == "" {
		return nil, fmt.Errorf("ALERT_TOPIC_ARN is required")
	}

	// Lambda connects through RDS Proxy, so a small pool is enough and
	// concurrent executions do not exhaust database connections.
	pool, err := store.Connect(ctx, os.Getenv("DATABASE_URL"), 2)
	if err != nil {
		return nil, err
	}
	sealer, err := webhook.NewSealer(os.Getenv("WEBHOOK_SECRET_KEY"))
	if err != nil {
		return nil, err
	}

	return &alert.Router{
		Notifier:      alert.SNSNotifier{Client: sns.NewFromConfig(awsCfg), TopicARN: topicARN},
		Subscriptions: store.New(pool),
		Opener:        sealer,
		Deliverer:     webhook.NewDeliverer(),
		Log:           log,
		Concurrency:   8,
	}, nil
}
