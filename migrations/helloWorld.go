package migrations

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	helloWorld = "hello-world"
)

func init() {
	Registry.registerMigration(helloWorld, newHelloWorld)
}

type hello struct{}

func newHelloWorld(opts MigrationOptions) (Migration, error) {
	return &hello{}, nil
}

func (c *hello) Execute(ctx context.Context, client *mongo.Client) error {
	fmt.Println("Hello World!")
	return nil
}
