package migrations

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/evergreen-ci/evergreen-migrations/migrations/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestGetNextTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.Connect(ctx)
	require.NoError(t, err)
	db := "migrations_test"
	collection := "tasks"
	ttlField := "create_time"
	require.NoError(t, testdata.InsertDocs(ctx, path.Join("testdata", "ttlCollection", "tasks.jsonl"), db, collection, client))
	defer func() {
		require.NoError(t, client.Database(db).Drop(ctx))
	}()

	for testName, testCase := range map[string]struct {
		job         TTLCollection
		now         time.Time
		expectedTTL time.Duration
	}{
		"SingleDocumentBatch": {
			job: TTLCollection{
				database:     db,
				collection:   collection,
				batchSize:    1,
				ttlDecrement: 24 * time.Hour,
				ttlField:     ttlField,
				goalTTL:      24 * time.Hour,
			},
			now:         time.Date(2020, 01, 05, 0, 0, 0, 0, time.UTC),
			expectedTTL: (3 * 24) * time.Hour,
		},
		"MultipleDocumentBatch": {
			job: TTLCollection{
				database:     db,
				collection:   collection,
				batchSize:    2,
				ttlDecrement: 24 * time.Hour,
				ttlField:     ttlField,
				goalTTL:      24 * time.Hour,
			},
			now:         time.Date(2020, 01, 05, 0, 0, 0, 0, time.UTC),
			expectedTTL: (2 * 24) * time.Hour,
		},
		"NextTTLLessThanGoal": {
			job: TTLCollection{
				database:     db,
				collection:   collection,
				batchSize:    5,
				ttlDecrement: 24 * time.Hour,
				ttlField:     ttlField,
				goalTTL:      3 * 24 * time.Hour,
			},
			now:         time.Date(2020, 01, 05, 0, 0, 0, 0, time.UTC),
			expectedTTL: 3 * 24 * time.Hour,
		},
	} {
		t.Run(testName, func(t *testing.T) {
			ttl, err := testCase.job.getNextTTL(ctx, testCase.now, client)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedTTL, ttl)
		})

	}
}

// TestExecute is an e2e test of the migration. It runs approximately two minutes.
// You can follow along its progress in a mongo shell by running db.tasks.getIndexes()
// to see the TTL on the create_time field and db.tasks.find() to see the remaining tasks.
func TestExecute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mongo.Connect(ctx)
	require.NoError(t, err)
	db := "migrations_test"
	collection := "tasks"
	ttlField := "create_time"
	require.NoError(t, testdata.InsertDocs(ctx, path.Join("testdata", "ttlCollection", "tasks.jsonl"), db, collection, client))
	defer func() {
		require.NoError(t, client.Database(db).Drop(ctx))
	}()

	now := time.Now().UTC()
	_, err = client.Database(db).Collection(collection).InsertOne(ctx, bson.M{ttlField: now})
	require.NoError(t, err)
	_, err = client.Database(db).Collection(collection).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: ttlField, Value: 1}}})
	require.NoError(t, err)

	ttlJob := TTLCollection{
		database:     db,
		collection:   collection,
		batchSize:    3,
		ttlDecrement: 24 * time.Hour,
		ttlField:     ttlField,
		goalTTL:      24 * time.Hour,
	}

	assert.NoError(t, ttlJob.Execute(ctx, client))
	task := struct {
		CreateTime time.Time `bson:"create_time"`
	}{}
	assert.NoError(t, client.Database(db).Collection(collection).FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.M{ttlField: 1})).Decode(&task))
	assert.True(t, task.CreateTime.Equal(now.Truncate(time.Millisecond)))
}

func TestNewTTLCollection(t *testing.T) {
	db := "migrations_test"
	collection := "tasks"
	batchSize := 10
	opts := MigrationOptions{
		Database:   db,
		Collection: collection,
		BatchSize:  batchSize,
	}

	t.Run("InvalidOptions", func(t *testing.T) {
		t.Setenv(goalTTLEnvVar, "1h")
		t.Setenv(ttlDecrementEnvVar, "1m")
		t.Setenv(ttlFieldEnvVar, "tasks")
		_, err := NewTTLCollection(MigrationOptions{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database name not specified")
		assert.Contains(t, err.Error(), "collection name not specified")
	})

	t.Run("NoEnvironmentVariables", func(t *testing.T) {
		_, err := NewTTLCollection(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("expected environment variable '%s' was not specified", goalTTLEnvVar))
		assert.Contains(t, err.Error(), fmt.Sprintf("expected environment variable '%s' was not specified", ttlDecrementEnvVar))
		assert.Contains(t, err.Error(), fmt.Sprintf("expected environment variable '%s' was not specified", ttlFieldEnvVar))
	})

	t.Run("InvalidDuration", func(t *testing.T) {
		t.Setenv(goalTTLEnvVar, "1h")
		t.Setenv(ttlDecrementEnvVar, "not_a_duration")
		t.Setenv(ttlFieldEnvVar, "tasks")
		_, err := NewTTLCollection(opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can't parse TTL decrement 'not_a_duration' as duration")
	})

	t.Run("ValidOptions", func(t *testing.T) {
		t.Setenv(goalTTLEnvVar, "1h")
		t.Setenv(ttlDecrementEnvVar, "1m")
		t.Setenv(ttlFieldEnvVar, "tasks")
		ttlJob, err := NewTTLCollection(opts)
		assert.NoError(t, err)
		assert.IsType(t, &TTLCollection{}, ttlJob)
	})
}
