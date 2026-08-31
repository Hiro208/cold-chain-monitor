// Command archiver copies raw telemetry from Kafka to S3.
package main

import (
	"fmt"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	"github.com/guangjiandong/cold-chain-monitor/internal/archive"
	"github.com/guangjiandong/cold-chain-monitor/internal/config"
	"github.com/guangjiandong/cold-chain-monitor/internal/platform"
	"github.com/guangjiandong/cold-chain-monitor/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "archiver:", err)
		os.Exit(1)
	}
}

func run() error {
	log := platform.NewLogger("archiver")
	cfg, err := config.LoadArchiver()
	if err != nil {
		return err
	}
	ctx, stop := platform.SignalContext()
	defer stop()

	// Credentials come from IRSA (IAM Roles for Service Accounts) on EKS and
	// from the usual env/profile chain locally. AWS_ENDPOINT_URL_S3 points
	// the client at MinIO/LocalStack in docker-compose.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = os.Getenv("S3_FORCE_PATH_STYLE") == "true" })

	runner, err := archive.NewRunner(stream.Conn(cfg.Kafka.Brokers, cfg.Kafka.TLS), cfg.Kafka.ConsumerGroup, cfg.Kafka.ReadingsTopic,
		archive.NewBatcher(cfg.Prefix, cfg.MaxObjectRows),
		archive.S3Uploader{Client: s3c, Bucket: cfg.Bucket},
		cfg.FlushEvery, log)
	if err != nil {
		return err
	}

	reg := platform.NewMetricsRegistry()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return platform.Serve(ctx, platform.NewHTTPServer(cfg.MetricsAddr, platform.MetricsHandler(reg)), platform.DefaultShutdownTimeout, log)
	})
	g.Go(func() error {
		log.Info("archiving", "topic", cfg.Kafka.ReadingsTopic, "bucket", cfg.Bucket, "prefix", cfg.Prefix)
		return runner.Run(ctx)
	})
	return g.Wait()
}
