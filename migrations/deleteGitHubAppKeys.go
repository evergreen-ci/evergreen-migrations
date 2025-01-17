package migrations

import (
	"context"
	"os"
	"strconv"

	"github.com/evergreen-ci/evergreen/model/githubapp"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	deleteGitHubAppKeysName = "deleteGitHubAppKeys"

	startAtGitHubAppAuthIDEnvVar = "START_AT_GITHUB_APP_AUTH_ID"
	githubAppAuthLimitEnvVar     = "GITHUB_APP_AUTH_LIMIT"
)

func init() {
	Registry.registerMigration(deleteGitHubAppKeysName, newDeleteGitHubAppKeys)
}

type deleteGitHubAppKeys struct {
	database string
}

func newDeleteGitHubAppKeys(opts MigrationOptions) (Migration, error) {
	catcher := grip.NewBasicCatcher()
	catcher.Wrap(opts.validate(), "invalid options")
	return &deleteGitHubAppKeys{
		database: opts.Database,
	}, catcher.Resolve()
}

// Execute runs a job to delete GitHub app private keys from the DB.
func (d *deleteGitHubAppKeys) Execute(ctx context.Context, client *mongo.Client) error {
	ids, err := d.findGitHubAppDocIDs(ctx, client)
	if err != nil {
		return errors.Wrap(err, "finding GitHub app auth IDs to update")
	}

	for _, id := range ids {
		grip.Infof("Deleting private key for GitHub app auth with ID '%s'", id)
		if _, err := client.Database(d.database).Collection(githubapp.GitHubAppAuthCollection).UpdateByID(ctx, id, bson.M{
			"$unset": bson.M{
				githubapp.GhAuthPrivateKeyKey: "",
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func (d *deleteGitHubAppKeys) findGitHubAppDocIDs(ctx context.Context, client *mongo.Client) ([]string, error) {
	query := bson.M{
		githubapp.GhAuthPrivateKeyKey: bson.M{"$exists": true},
	}
	opts := options.Find().SetProjection(bson.M{githubapp.GhAuthIdKey: 1})

	if startAtID := os.Getenv(startAtGitHubAppAuthIDEnvVar); startAtID != "" {
		query[githubapp.GhAuthIdKey] = bson.M{"$gte": startAtID}
		opts.SetSort(bson.M{githubapp.GhAuthIdKey: 1})
	}

	var docs []githubapp.GithubAppAuth
	if limitStr := os.Getenv(githubAppAuthLimitEnvVar); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing limit '%s'", limitStr)
		}
		opts.SetLimit(int64(limit))
		docs = make([]githubapp.GithubAppAuth, 0, limit)
	} else {
		docs = []githubapp.GithubAppAuth{}
	}

	cur, err := client.Database(d.database).Collection(githubapp.GitHubAppAuthCollection).Find(ctx, query, opts)
	if err != nil {
		return nil, errors.Wrap(err, "finding GitHub app auth documents")
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, errors.Wrap(err, "iterating over GitHub app auth documents")
	}

	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.Id)
	}

	return ids, nil
}
