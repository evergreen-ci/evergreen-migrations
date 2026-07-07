package migrations

import (
	"bytes"
	"context"

	"github.com/evergreen-ci/evergreen/model"
	"github.com/evergreen-ci/evergreen/model/event"
	"github.com/mongodb/anser/bsonutil"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	redactProjectEventSecretsName = "redactProjectEventSecrets"

	// Optional env vars to control what project ID to start at for branch
	// project and repo refs.
	startAtProjectIDEnvVar = "START_AT_PROJECT_ID"
	startAtRepoIDEnvVar    = "START_AT_REPO_ID"
	// Limit how many projects can be processed by the job.
	projectLimitEnvVar = "PROJECT_LIMIT"
	// Limit how many events can be processed for a single project.
	eventLimitEnvVar = "EVENT_LIMIT"
)

var (
	eventIDKey   = bsonutil.MustHaveTag(event.EventLogEntry{}, "ID")
	eventTypeKey = bsonutil.MustHaveTag(event.EventLogEntry{}, "EventType")
)

func init() {
	Registry.registerMigration(redactProjectEventSecretsName, newRedactProjectEventSecrets)
}

// redactProjectEventSecrets is a migration to retroactively redact project
// secret values from project modifications in the event log.
type redactProjectEventSecrets struct {
	database string
}

func newRedactProjectEventSecrets(opts MigrationOptions) (Migration, error) {
	catcher := grip.NewBasicCatcher()
	catcher.Add(errors.Wrap(opts.validate(), "invalid options"))

	return &redactProjectEventSecrets{
		database: opts.Database,
	}, catcher.Resolve()
}

func (c *redactProjectEventSecrets) Execute(ctx context.Context, client *mongo.Client) error {
	return forEachProject(ctx, client, c.database, func(projectID string, eventLimit int) error {
		if err := c.redactForProject(ctx, client, projectID, eventLimit); err != nil {
			return errors.Wrapf(err, "redacting project vars from events for project '%s'", projectID)
		}
		return nil
	})
}

func (c *redactProjectEventSecrets) redactForProject(ctx context.Context, client *mongo.Client, projectID string, eventLimit int) error {
	grip.Infof(ctx, "Redacting project vars from events for project: %s\n", projectID)

	projModificationEventsQuery := bson.M{
		event.ResourceIdKey:   projectID,
		event.ResourceTypeKey: event.EventResourceTypeProject,
		eventTypeKey:          event.EventTypeProjectModified,
	}

	findOpts := options.Find().SetSort(bson.M{event.TimestampKey: 1})
	if eventLimit > 0 {
		findOpts.SetLimit(int64(eventLimit))
	}
	cur, err := client.Database(c.database).Collection(event.EventCollection).Find(ctx, projModificationEventsQuery, findOpts)
	if err != nil {
		return errors.Wrap(err, "finding project modification events")
	}

	for cur.Next(ctx) {
		var e model.ProjectChangeEventEntry
		if err := cur.Decode(&e); err != nil {
			return errors.Wrap(err, "decoding event")
		}

		originalEventData := e.Data.(*model.ProjectChangeEvent)
		if originalEventData == nil {
			continue
		}
		beforeGitHubAppAuth := originalEventData.Before.GitHubAppAuth.PrivateKey
		afterGitHubAppAuth := originalEventData.After.GitHubAppAuth.PrivateKey

		// Redact the project secrets from the event.
		originalEventData.RedactSecrets()

		eventData, ok := e.Data.(*model.ProjectChangeEvent)
		if !ok {
			continue
		}
		if eventData == nil {
			continue
		}

		setFields := bson.M{}
		if len(eventData.Before.Vars.Vars) > 0 {
			setFields["data.before.vars.vars"] = eventData.Before.Vars.Vars
		}
		if len(eventData.After.Vars.Vars) > 0 {
			setFields["data.after.vars.vars"] = eventData.After.Vars.Vars
		}
		if !bytes.Equal(eventData.Before.GitHubAppAuth.PrivateKey, beforeGitHubAppAuth) {
			setFields["data.before.github_app_auth.private_key"] = eventData.Before.GitHubAppAuth.PrivateKey
		}
		if !bytes.Equal(eventData.After.GitHubAppAuth.PrivateKey, afterGitHubAppAuth) {
			setFields["data.after.github_app_auth.private_key"] = eventData.After.GitHubAppAuth.PrivateKey
		}
		if len(setFields) == 0 {
			continue
		}

		if _, err := client.Database(c.database).Collection(event.EventCollection).UpdateOne(ctx,
			bson.M{eventIDKey: e.ID},
			bson.M{"$set": setFields}); err != nil {
			return errors.Wrapf(err, "updating project modification event data for event '%s'", e.ID)
		}
	}
	if err := cur.Err(); err != nil {
		return errors.Wrap(cur.Err(), "iterating over project modification events")
	}

	return nil
}
