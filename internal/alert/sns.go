package alert

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

// SNSNotifier publishes a human-readable alert to an SNS topic. Message
// attributes let subscribers filter, e.g. only TOO_WARM for vaccines.
type SNSNotifier struct {
	Client   *sns.Client
	TopicARN string
}

// Notify implements Notifier.
func (n SNSNotifier) Notify(ctx context.Context, ev stream.BreachEvent) error {
	b := ev.Breach
	subject := fmt.Sprintf("[%s] %s %s", ev.Type, b.ContainerID, b.Kind)
	msg := fmt.Sprintf("Container %s (%s, owner %s) %s: peak %.2f °C vs limit %.2f °C, started %s. Detected %d ms after the reading.",
		b.ContainerID, ev.Commodity, ev.Owner, ev.Type, b.PeakTempC, b.LimitC, b.StartedAt.Format("2006-01-02 15:04:05Z07:00"), ev.LatencyMs)
	_, err := n.Client.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(n.TopicARN),
		Subject:  aws.String(subject),
		Message:  aws.String(msg),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"event_type": {DataType: aws.String("String"), StringValue: aws.String(string(ev.Type))},
			"kind":       {DataType: aws.String("String"), StringValue: aws.String(string(b.Kind))},
			"owner":      {DataType: aws.String("String"), StringValue: aws.String(ev.Owner)},
			"commodity":  {DataType: aws.String("String"), StringValue: aws.String(ev.Commodity)},
		},
	})
	return err
}
