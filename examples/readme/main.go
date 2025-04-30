package main

import (
	"context"
	"fmt"
	"time"

	mg "github.com/hypermodeinc/modusgraph"
)

// This example is featured on the repo README

type TestEntity struct {
	Name        string    `json:"name,omitempty" dgraph:"index=exact"`
	Description string    `json:"description,omitempty" dgraph:"index=term"`
	CreatedAt   time.Time `json:"createdAt,omitempty"`

	// UID is a required field for nodes
	UID string `json:"uid,omitempty"`
	// DType is a required field for nodes, will get populated with the struct name
	DType []string `json:"dgraph.type,omitempty"`
}

func main() {
	client, err := mg.NewClient("file:///tmp/modusgraph", mg.WithAutoSchema(true))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	entity := TestEntity{
		Name:        "Test Entity",
		Description: "This is a test entity",
		CreatedAt:   time.Now(),
	}

	ctx := context.Background()
	err = client.Insert(ctx, &entity)

	if err != nil {
		panic(err)
	}
	fmt.Println("Insert successful, entity UID:", entity.UID)

	// Query the entity
	var result TestEntity
	err = client.Get(ctx, &result, entity.UID)
	if err != nil {
		panic(err)
	}
	fmt.Println("Query successful, entity:", result.UID)
}
