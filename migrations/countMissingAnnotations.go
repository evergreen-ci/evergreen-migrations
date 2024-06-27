package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/evergreen-ci/evergreen"
	"github.com/evergreen-ci/evergreen/model/annotations"
	"github.com/evergreen-ci/evergreen/model/task"
	"github.com/mongodb/anser/bsonutil"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	missingAnnotationCountName = "countMissingAnnotations"
)

func init() {
	Registry.registerMigration(missingAnnotationCountName, NewCountMissingAnnotations)
}

type CountMissingAnnotations struct {
	database   string
	collection string
}

func NewCountMissingAnnotations(opts MigrationOptions) (Migration, error) {
	catcher := grip.NewBasicCatcher()
	catcher.Add(errors.Wrap(opts.validate(), "invalid options"))

	if opts.Collection == "" {
		catcher.Add(errors.New("collection name not specified"))
	}

	return &CountMissingAnnotations{
		database:   opts.Database,
		collection: opts.Collection,
	}, catcher.Resolve()
}

func (c *CountMissingAnnotations) Execute(ctx context.Context, client *mongo.Client) error {
	timeToCheck := time.Now().AddDate(0, 0, -30)
	query := bson.M{
		"$match": bson.M{
			task.ProjectKey:   "mongodb-mongo-v8.0",
			task.RequesterKey: evergreen.RepotrackerVersionRequester,
			task.StatusKey:    evergreen.TaskFailed,
			bsonutil.GetDottedKeyName(task.DetailsKey, task.TaskEndDetailType): evergreen.CommandTypeTest,
			task.HasAnnotationsKey: false,
			task.CreateTimeKey:     bson.M{"$gte": timeToCheck},
		},
	}

	cursor, err := client.Database(c.database).Collection(task.Collection).Find(ctx, query)
	if err != nil {
		return errors.Wrap(err, "aggregating tasks")
	}
	tasksToCheck := []task.Task{}
	err = cursor.All(ctx, &tasksToCheck)
	if err != nil {
		return errors.Wrap(err, "reading cursor")
	}
	fmt.Printf("Found %d  task(s) to check since '%s' (last 30 days).\n", len(tasksToCheck), timeToCheck.String())

	taskIdsWithoutAnnotations := []string{}
	for _, t := range tasksToCheck {
		query := bson.M{
			annotations.TaskIdKey:        t.Id,
			annotations.TaskExecutionKey: t.Execution,
		}

		res := client.Database(c.database).Collection(annotations.Collection).FindOne(ctx, query)
		if res.Err() != nil && res.Err() == mongo.ErrNoDocuments {
			taskIdsWithoutAnnotations = append(taskIdsWithoutAnnotations, t.Id)
		}
	}

	fmt.Printf("%d task(s) without annotations: %v\n", len(taskIdsWithoutAnnotations), taskIdsWithoutAnnotations)
	return nil
}
