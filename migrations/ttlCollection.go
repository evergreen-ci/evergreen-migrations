package migrations

import (
	"context"
	"os"
	"time"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	ttlMigrationName = "ttlCollection"
	ttlWaitSleep     = 10 * time.Second
	defaultBatchSize = 1000000

	goalTTLEnvVar      = "GOAL_TTL"
	ttlDecrementEnvVar = "TTL_DECREMENT"
	ttlFieldEnvVar     = "TTL_FIELD"
)

func init() {
	Registry.registerMigration(ttlMigrationName, NewTTLCollection)
}

type TTLCollection struct {
	database     string
	collection   string
	batchSize    int
	goalTTL      time.Duration
	ttlDecrement time.Duration
	ttlField     string
}

func NewTTLCollection(opts MigrationOptions) (Migration, error) {
	catcher := grip.NewBasicCatcher()
	catcher.Add(errors.Wrap(opts.validate(), "invalid options"))

	if opts.Collection == "" {
		catcher.Add(errors.New("collection ttlMigrationName not specified"))
	}

	if opts.BatchSize == 0 {
		opts.BatchSize = defaultBatchSize
	}

	var err error
	var goalTTL time.Duration
	goalTTLString, ok := os.LookupEnv(goalTTLEnvVar)
	if !ok {
		catcher.Add(errors.Errorf("expected environment variable '%s' was not specified", goalTTLEnvVar))
	} else {
		goalTTL, err = time.ParseDuration(goalTTLString)
		catcher.Add(errors.Wrapf(err, "can't parse goal TTL '%s' as duration", goalTTLString))
	}

	var ttlDecrement time.Duration
	ttlDecrementString, ok := os.LookupEnv(ttlDecrementEnvVar)
	if !ok {
		catcher.Add(errors.Errorf("expected environment variable '%s' was not specified", ttlDecrementEnvVar))
	} else {
		ttlDecrement, err = time.ParseDuration(ttlDecrementString)
		catcher.Add(errors.Wrapf(err, "can't parse TTL decrement '%s' as duration", ttlDecrementString))
	}

	ttlField, ok := os.LookupEnv(ttlFieldEnvVar)
	if !ok {
		catcher.Add(errors.Errorf("expected environment variable '%s' was not specified", ttlFieldEnvVar))
	}

	return &TTLCollection{
		database:     opts.Database,
		collection:   opts.Collection,
		batchSize:    opts.BatchSize,
		goalTTL:      goalTTL,
		ttlDecrement: ttlDecrement,
		ttlField:     ttlField,
	}, catcher.Resolve()
}

func (t *TTLCollection) Execute(ctx context.Context, client *mongo.Client) error {
	for {
		// Capture the time at the beginning of this iteration so we aren't working against a moving target.
		now := time.Now()

		nextTTL, err := t.getNextTTL(ctx, now, client)
		if err != nil {
			return errors.Wrap(err, "getting next TTL")
		}
		grip.Infof("TTL corresponds to '%s'", now.Add(-nextTTL).Format(time.DateTime))

		res := client.Database(t.database).RunCommand(ctx, bson.D{
			{Key: "collMod", Value: t.collection},
			{Key: "index", Value: bson.M{
				"keyPattern":         bson.M{t.ttlField: 1},
				"expireAfterSeconds": int(nextTTL.Seconds()),
			}},
		})
		if err := res.Err(); err != nil {
			return errors.Wrap(err, "setting collection TTL")
		}

		if err := t.waitForTTL(ctx, now, nextTTL, client); err != nil {
			return errors.Wrap(err, "waiting for TTL job")
		}

		if nextTTL == t.goalTTL {
			return nil
		}
	}
}

func (t *TTLCollection) getNextTTL(ctx context.Context, now time.Time, client *mongo.Client) (time.Duration, error) {
	collection := client.Database(t.database).Collection(t.collection)

	// Initial TTL is the duration since the creation of the oldest document.
	raw, err := collection.FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.M{t.ttlField: 1})).Raw()
	if err != nil {
		return 0, errors.Wrap(err, "getting oldest document")
	}
	creationTime, ok := raw.Lookup(t.ttlField).TimeOK()
	if !ok {
		return 0, errors.Errorf("TTL field '%s' does not exist or is not a time", t.ttlField)
	}
	ttl := now.Sub(creationTime)

	for {
		count, err := collection.CountDocuments(ctx, bson.M{t.ttlField: bson.M{"$lt": now.Add(-ttl)}})
		if err != nil {
			return 0, errors.Wrap(err, "getting document count")
		}
		if int(count) >= t.batchSize || ttl <= t.goalTTL {
			break
		}
		ttl -= t.ttlDecrement
	}

	if ttl < t.goalTTL {
		ttl = t.goalTTL
	}

	return ttl, nil
}

func (t *TTLCollection) waitForTTL(ctx context.Context, now time.Time, ttl time.Duration, client *mongo.Client) error {
	for {
		res := client.Database(t.database).Collection(t.collection).FindOne(ctx, bson.M{t.ttlField: bson.M{"$lt": now.Add(-ttl)}})
		if err := res.Err(); err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return errors.Wrap(err, "checking for remaining documents to TTL")
		}
		time.Sleep(ttlWaitSleep)
	}
}
