package migrations

import (
	"context"
	"os"
	"strconv"

	"github.com/evergreen-ci/evergreen/model"
	"github.com/evergreen-ci/evergreen/model/event"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	redactWebhookEventSecretsName = "redactWebhookEventSecrets"

	startAtWebhookProjectIDEnvVar = "START_AT_PROJECT_ID"
	startAtWebhookRepoIDEnvVar    = "START_AT_REPO_ID"
	webhookProjectLimitEnvVar     = "PROJECT_LIMIT"
	webhookEventLimitEnvVar       = "EVENT_LIMIT"
)

func init() {
	Registry.registerMigration(redactWebhookEventSecretsName, newRedactWebhookEventSecrets)
}

// redactWebhookEventSecrets is a migration to retroactively redact webhook
// secrets from project modification events in the event log.
type redactWebhookEventSecrets struct {
	database string
}

func newRedactWebhookEventSecrets(opts MigrationOptions) (Migration, error) {
	catcher := grip.NewBasicCatcher()
	catcher.Add(errors.Wrap(opts.validate(), "invalid options"))
	return &redactWebhookEventSecrets{
		database: opts.Database,
	}, catcher.Resolve()
}

func (c *redactWebhookEventSecrets) Execute(ctx context.Context, client *mongo.Client) error {
	collInfos := []struct {
		name      string
		startAtID string
	}{
		{
			name:      "project_ref",
			startAtID: os.Getenv(startAtWebhookProjectIDEnvVar),
		},
		{
			name:      "repo_ref",
			startAtID: os.Getenv(startAtWebhookRepoIDEnvVar),
		},
	}

	var projectLimit int
	if projectLimitStr := os.Getenv(webhookProjectLimitEnvVar); projectLimitStr != "" {
		var err error
		projectLimit, err = strconv.Atoi(projectLimitStr)
		if err != nil {
			return errors.Wrapf(err, "parsing project limit '%s' from env var '%s'", projectLimitStr, webhookProjectLimitEnvVar)
		}
	}

	var eventLimit int
	if eventLimitStr := os.Getenv(webhookEventLimitEnvVar); eventLimitStr != "" {
		var err error
		eventLimit, err = strconv.Atoi(eventLimitStr)
		if err != nil {
			return errors.Wrapf(err, "parsing event limit '%s' from env var '%s'", eventLimitStr, webhookEventLimitEnvVar)
		}
	}

	numProjectsProcessed := 0
	for _, collInfo := range collInfos {
		q := bson.M{}
		if collInfo.startAtID != "" {
			q["_id"] = bson.M{"$gte": collInfo.startAtID}
			grip.Infof("Starting at project '%s' in collection '%s'\n", collInfo.startAtID, collInfo.name)
		}
		findOpts := options.Find().SetSort(bson.M{"_id": 1}).SetProjection(bson.M{"_id": 1})
		if projectLimit > 0 {
			findOpts.SetLimit(int64(projectLimit - numProjectsProcessed))
		}
		cur, err := client.Database(c.database).Collection(collInfo.name).Find(ctx, q, findOpts)
		if err != nil {
			return errors.Wrapf(err, "finding project refs in collection '%s'", collInfo.name)
		}

		for cur.Next(ctx) {
			if projectLimit > 0 && numProjectsProcessed >= projectLimit {
				grip.Infof("Reached limit of %d projects to process, stopping job execution.\n", projectLimit)
				return nil
			}

			var pRef model.ProjectRef
			if err := cur.Decode(&pRef); err != nil {
				return errors.Wrap(err, "decoding project ref")
			}
			projectID := pRef.Id
			if projectID == "" {
				return errors.New("project ID is empty")
			}
			if err := c.redactWebhookSecretsForProject(ctx, client, projectID, eventLimit); err != nil {
				return errors.Wrapf(err, "redacting webhook secrets from events for project '%s'", projectID)
			}

			numProjectsProcessed++
		}

		if err := cur.Err(); err != nil {
			return errors.Wrap(err, "iterating over project refs")
		}
	}

	return nil
}

func (c *redactWebhookEventSecrets) redactWebhookSecretsForProject(ctx context.Context, client *mongo.Client, projectID string, eventLimit int) error {
	grip.Infof("Redacting webhook secrets from events for project: %s\n", projectID)

	query := bson.M{
		event.ResourceIdKey:   projectID,
		event.ResourceTypeKey: event.EventResourceTypeProject,
		eventTypeKey:          event.EventTypeProjectModified,
	}

	findOpts := options.Find().SetSort(bson.M{event.TimestampKey: 1})
	if eventLimit > 0 {
		findOpts.SetLimit(int64(eventLimit))
	}
	cur, err := client.Database(c.database).Collection(event.EventCollection).Find(ctx, query, findOpts)
	if err != nil {
		return errors.Wrap(err, "finding project modification events")
	}

	for cur.Next(ctx) {
		var e model.ProjectChangeEventEntry
		if err := cur.Decode(&e); err != nil {
			return errors.Wrap(err, "decoding event")
		}

		eventData, ok := e.Data.(*model.ProjectChangeEvent)
		if !ok || eventData == nil {
			continue
		}

		if len(eventData.Before.Subscriptions) == 0 && len(eventData.After.Subscriptions) == 0 {
			continue
		}

		// RedactSecrets handles redaction of webhook secrets and Authorization headers.
		changeEvent := model.ProjectChangeEvents{e}
		changeEvent.RedactSecrets()

		if _, err := client.Database(c.database).Collection(event.EventCollection).UpdateOne(ctx,
			bson.M{eventIDKey: e.ID},
			bson.M{"$set": bson.M{
				"data.before.subscriptions": eventData.Before.Subscriptions,
				"data.after.subscriptions":  eventData.After.Subscriptions,
			}}); err != nil {
			return errors.Wrapf(err, "updating event '%s' with redacted webhook secrets", e.ID)
		}
	}
	if err := cur.Err(); err != nil {
		return errors.Wrap(err, "iterating over project modification events")
	}

	return nil
}
