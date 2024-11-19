package migrations

import (
	"context"
	"os"
	"strconv"

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
	collInfos := []struct {
		name      string
		startAtID string
	}{
		{
			name:      "project_ref",
			startAtID: os.Getenv(startAtProjectIDEnvVar),
		},
		{
			name:      "repo_ref",
			startAtID: os.Getenv(startAtRepoIDEnvVar),
		},
	}

	var projectLimit int
	if projectLimitStr := os.Getenv(projectLimitEnvVar); projectLimitStr != "" {
		var err error
		projectLimit, err = strconv.Atoi(projectLimitStr)
		if err != nil {
			return errors.Wrapf(err, "parsing project limit '%s' from env var '%s'", projectLimitStr, projectLimitEnvVar)
		}
	}

	numProjectsProcessed := 0
	for _, collInfo := range collInfos {
		q := bson.M{}
		if collInfo.startAtID != "" {
			q["_id"] = bson.M{"$gte": collInfo.startAtID}
			grip.Infof("Starting at project '%s' in collection '%s'\n", collInfo.startAtID, collInfo.name)
		}
		// Sort by _id to iterate in a predictable order. This makes it easier to
		// resume from a specific project if the migration fails partway through.
		findOpts := options.Find().SetSort(bson.M{"_id": 1}).SetProjection(bson.M{"_id": 1})
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
			if err := c.redactForProject(ctx, client, projectID); err != nil {
				return errors.Wrapf(err, "redacting project vars from events for project '%s'", projectID)
			}

			numProjectsProcessed++
		}

		if err := cur.Err(); err != nil {
			return errors.Wrap(err, "iterating over project refs")
		}
	}

	return nil
}

func (c *redactProjectEventSecrets) redactForProject(ctx context.Context, client *mongo.Client, projectID string) error {
	grip.Infof("Redacting project vars from events for project: %s\n", projectID)

	projModificationEventsQuery := bson.M{
		event.ResourceIdKey:   projectID,
		event.ResourceTypeKey: event.EventResourceTypeProject,
		eventTypeKey:          event.EventTypeProjectModified,
	}

	findOpts := options.Find().SetSort(bson.M{event.TimestampKey: 1})
	cur, err := client.Database(c.database).Collection(event.EventCollection).Find(ctx, projModificationEventsQuery, findOpts)
	if err != nil {
		return errors.Wrap(err, "finding project modification events")
	}

	for cur.Next(ctx) {
		var e event.EventLogEntry
		if err := cur.Decode(&e); err != nil {
			return errors.Wrap(err, "decoding event")
		}

		// Redact the project secrets from the event.
		changeEvent := model.ProjectChangeEventEntry{
			EventLogEntry: e,
		}
		changeEvents := model.ProjectChangeEvents{changeEvent}
		changeEvents.RedactSecrets()

		eventData, ok := e.Data.(*model.ProjectChangeEvent)
		if !ok {
			continue
		}
		if eventData == nil {
			continue
		}

		setFields := bson.M{}
		if len(eventData.Before.Vars.Vars) > 0 {
			setFields["data.before.vars.vars"] = eventData.Before.Vars
		}
		if len(eventData.After.Vars.Vars) > 0 {
			setFields["data.after.vars.vars"] = eventData.After.Vars
		}
		if len(eventData.Before.GitHubAppAuth.PrivateKey) > 0 {
			setFields["data.before.github_app_auth.private_key"] = eventData.Before.GitHubAppAuth.PrivateKey
		}
		if len(eventData.After.GitHubAppAuth.PrivateKey) > 0 {
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
