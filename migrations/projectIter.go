package migrations

import (
	"context"
	"os"
	"strconv"

	"github.com/evergreen-ci/evergreen/model"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// forEachProject iterates over all branch project and repo refs and calls fn
// for each one. It respects PROJECT_LIMIT, EVENT_LIMIT, START_AT_PROJECT_ID,
// and START_AT_REPO_ID environment variables.
func forEachProject(ctx context.Context, client *mongo.Client, database string, fn func(projectID string, eventLimit int) error) error {
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

	var eventLimit int
	if eventLimitStr := os.Getenv(eventLimitEnvVar); eventLimitStr != "" {
		var err error
		eventLimit, err = strconv.Atoi(eventLimitStr)
		if err != nil {
			return errors.Wrapf(err, "parsing event limit '%s' from env var '%s'", eventLimitStr, eventLimitEnvVar)
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
		if projectLimit > 0 {
			findOpts.SetLimit(int64(projectLimit - numProjectsProcessed))
		}
		cur, err := client.Database(database).Collection(collInfo.name).Find(ctx, q, findOpts)
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
			if err := fn(projectID, eventLimit); err != nil {
				return err
			}

			numProjectsProcessed++
		}

		if err := cur.Err(); err != nil {
			return errors.Wrap(err, "iterating over project refs")
		}
	}

	return nil
}
