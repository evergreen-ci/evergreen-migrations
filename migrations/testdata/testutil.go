package testdata

import (
	"bufio"
	"context"
	"os"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func InsertDocs(ctx context.Context, path, db, collection string, client *mongo.Client) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "opening path")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Set the max buffer size to the max size of a Mongo document (16MB).
	scanner.Buffer(make([]byte, 4096), 16*1024*1024)
	var count int
	for scanner.Scan() {
		bytes := scanner.Bytes()

		var doc bson.D
		if err := bson.UnmarshalExtJSON(bytes, false, &doc); err != nil {
			return errors.Wrapf(err, "unmarshaling test data line %d", count)
		}
		if _, err = client.Database(db).Collection(collection).InsertOne(ctx, doc); err != nil {
			return errors.Wrapf(err, "inserting test data line %d", count)
		}

		count++
	}

	return scanner.Err()
}
