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

const (
	deleteProjectVarsName = "deleteProjectVars"

	startAtProjectVarsAuthIDEnvVar = "START_AT_PROJECT_ID"
	projectVarsLimitEnvVar         = "PROJECT_LIMIT"
)

func init() {
	Registry.registerMigration(deleteProjectVarsName, newDeleteProjectVars)
}

type deleteProjectVars struct {
	database string
}

func newDeleteProjectVars(opts MigrationOptions) (Migration, error) {
	catcher := grip.NewBasicCatcher()
	catcher.Wrap(opts.validate(), "invalid options")
	return &deleteProjectVars{
		database: opts.Database,
	}, catcher.Resolve()
}

// Execute runs a job to delete project vars from the DB.
func (d *deleteProjectVars) Execute(ctx context.Context, client *mongo.Client) error {
	ids, err := d.findProjectVarsDocIDs(ctx, client)
	if err != nil {
		return errors.Wrap(err, "finding project var doc IDs to update")
	}

	for _, id := range ids {
		grip.Infof("Deleting project vars for project with ID '%s'", id)
		if _, err := client.Database(d.database).Collection(model.ProjectVarsCollection).UpdateByID(ctx, id, bson.M{
			"$unset": bson.M{
				"vars": 1,
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func (d *deleteProjectVars) findProjectVarsDocIDs(ctx context.Context, client *mongo.Client) ([]string, error) {
	query := bson.M{
		"vars": bson.M{"$exists": true},
	}
	opts := options.Find().SetProjection(bson.M{"_id": 1})

	if startAtID := os.Getenv(startAtProjectVarsAuthIDEnvVar); startAtID != "" {
		query["_id"] = bson.M{"$gte": startAtID}
		opts.SetSort(bson.M{"_id": 1})
	}

	var docs []model.ProjectVars
	if limitStr := os.Getenv(projectVarsLimitEnvVar); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing limit '%s'", limitStr)
		}
		opts.SetLimit(int64(limit))
		docs = make([]model.ProjectVars, 0, limit)
	} else {
		docs = []model.ProjectVars{}
	}

	cur, err := client.Database(d.database).Collection(model.ProjectVarsCollection).Find(ctx, query, opts)
	if err != nil {
		return nil, errors.Wrap(err, "finding project var documents")
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, errors.Wrap(err, "iterating over project var documents")
	}

	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.Id)
	}

	return ids, nil
}
