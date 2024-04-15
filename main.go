package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/evergreen-ci/migrations/migrations"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	urlFlag        = "url"
	dbFlag         = "db"
	scriptFlag     = "script"
	collectionFlag = "collection"
	batchSizeFlag  = "batch-size"

	awsAuthMechanism        = "MONGODB-AWS"
	mongoExternalAuthSource = "$external"
)

func main() {
	app := cli.NewApp()
	app.Name = "migrator"
	app.Usage = "Run migrations against a database"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     urlFlag,
			Usage:    "Database URL",
			Required: true,
		},
		cli.StringFlag{
			Name:     dbFlag,
			Usage:    "Database name",
			Required: true,
		},
		cli.StringFlag{
			Name:     scriptFlag,
			Usage:    "Name of the script to run",
			Required: true,
		},
		cli.StringFlag{
			Name:  collectionFlag,
			Usage: "Collection to run the script against",
		},
		cli.IntFlag{
			Name:  batchSizeFlag,
			Usage: "Batch size for the script to process at once",
		},
	}
	app.Action = func(c *cli.Context) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			exitCh := make(chan os.Signal, 1)
			signal.Notify(exitCh, syscall.SIGTERM)
			<-exitCh
			cancel()
		}()

		clientOps := options.Client().ApplyURI(c.String(urlFlag)).SetAuth(options.Credential{
			AuthMechanism: awsAuthMechanism,
			AuthSource:    mongoExternalAuthSource,
		})
		client, err := mongo.Connect(ctx, clientOps)
		if err != nil {
			return errors.Wrap(err, "getting mongo client")
		}

		migration, err := migrations.Registry.Migration(c.String(scriptFlag), migrations.MigrationOptions{
			Database:   c.String(dbFlag),
			Collection: c.String(collectionFlag),
			BatchSize:  c.Int(batchSizeFlag),
		})
		if err != nil {
			return errors.Wrap(err, "getting migration script")
		}

		return migration.Execute(ctx, client)
	}
}
