package migrations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	helloWorld = "hello-world"
)

func init() {
	Registry.registerMigration(helloWorld, newHelloWorld)
}

// hello connects to the database and prints the result of a findOne on the specified collection.
type hello struct {
	database   string
	collection string
}

func newHelloWorld(opts MigrationOptions) (Migration, error) {
	return &hello{
		database:   opts.Database,
		collection: opts.Collection,
	}, nil
}

func (h *hello) Execute(ctx context.Context, client *mongo.Client) error {
	res := client.Database(h.database).Collection(h.collection).FindOne(ctx, bson.M{})
	doc, err := res.Raw()
	if err != nil {
		return fmt.Errorf("finding document in collection '%s': %w", h.collection, err)
	}

	jsonString, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return fmt.Errorf("marshalling document to json: %w", err)
	}

	var jsonBuffer bytes.Buffer
	if err := json.Indent(&jsonBuffer, jsonString, "", "  "); err != nil {
		return fmt.Errorf("pretty printing json output: %w", err)
	}

	fmt.Println(jsonBuffer.String())
	return nil
}
