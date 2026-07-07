package migrations

import (
	"context"

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
	return forEachProject(ctx, client, c.database, func(projectID string, eventLimit int) error {
		if err := c.redactWebhookSecretsForProject(ctx, client, projectID, eventLimit); err != nil {
			return errors.Wrapf(err, "redacting webhook secrets from events for project '%s'", projectID)
		}
		return nil
	})
}

func (c *redactWebhookEventSecrets) redactWebhookSecretsForProject(ctx context.Context, client *mongo.Client, projectID string, eventLimit int) error {
	grip.Infof(ctx, "Redacting webhook secrets from events for project: %s\n", projectID)

	query := bson.M{
		event.ResourceIdKey:   projectID,
		event.ResourceTypeKey: event.EventResourceTypeProject,
		eventTypeKey: bson.M{"$in": []string{
			event.EventTypeProjectModified,
			event.EventTypeProjectAttachedToRepo,
			event.EventTypeProjectDetachedFromRepo,
		}},
	}

	findOpts := options.Find().SetSort(bson.M{event.TimestampKey: 1})
	if eventLimit > 0 {
		findOpts.SetLimit(int64(eventLimit))
	}
	cur, err := client.Database(c.database).Collection(event.EventCollection).Find(ctx, query, findOpts)
	if err != nil {
		return errors.Wrap(err, "finding project modification events")
	}

	numEventsFound := 0
	var numEventsUpdated int64
	for cur.Next(ctx) {
		numEventsFound++
		var e model.ProjectChangeEventEntry
		if err := cur.Decode(&e); err != nil {
			return errors.Wrap(err, "decoding event")
		}

		eventData, ok := e.Data.(*model.ProjectChangeEvent)
		if !ok || eventData == nil {
			continue
		}

		// Skip events that have no webhook secrets to redact.
		if !hasWebhookSecretsToRedact(eventData.Before.Subscriptions) && !hasWebhookSecretsToRedact(eventData.After.Subscriptions) {
			continue
		}

		eventData.RedactSecrets()

		result, err := client.Database(c.database).Collection(event.EventCollection).UpdateOne(ctx,
			bson.M{eventIDKey: e.ID},
			bson.M{"$set": bson.M{
				"data.before.subscriptions": eventData.Before.Subscriptions,
				"data.after.subscriptions":  eventData.After.Subscriptions,
			}})
		if err != nil {
			return errors.Wrapf(err, "updating event '%s' with redacted webhook secrets", e.ID)
		}
		numEventsUpdated += result.ModifiedCount
	}
	if err := cur.Err(); err != nil {
		return errors.Wrap(err, "iterating over project modification events")
	}

	grip.Infof(ctx, "Finished redacting webhook secrets for project '%s': found %d events, updated %d events.\n", projectID, numEventsFound, numEventsUpdated)

	return nil
}

// hasWebhookSecretsToRedact returns true if any of the given subscriptions are
// webhook type and have a non-empty secret or Authorization header that needs to be redacted.
func hasWebhookSecretsToRedact(subscriptions []event.Subscription) bool {
	for _, sub := range subscriptions {
		if sub.Subscriber.Type != event.EvergreenWebhookSubscriberType {
			continue
		}
		webhookSub, ok := sub.Subscriber.Target.(*event.WebhookSubscriber)
		if ok && webhookSub != nil && (len(webhookSub.Secret) > 0 || webhookSub.GetHeader("Authorization") != "") {
			return true
		}
	}
	return false
}
