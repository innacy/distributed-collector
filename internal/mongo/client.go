package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
}

func Connect(ctx context.Context, uri, database string) (*Client, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	return &Client{
		client:   client,
		database: client.Database(database),
	}, nil
}

func (c *Client) Collection(name string) *mongo.Collection {
	return c.database.Collection(name)
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}
