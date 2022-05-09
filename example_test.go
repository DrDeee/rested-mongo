package mongo_test

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/clarify/rested/resource"
	"github.com/clarify/rested/rest"
	"github.com/clarify/rested/schema"
	mgo "github.com/drdeee/rested-mongo"
	"github.com/rs/cors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	user = schema.Schema{
		Fields: schema.Fields{
			"id":      schema.IDField,
			"created": schema.CreatedField,
			"updated": schema.UpdatedField,
			"name": {
				Required:   true,
				Filterable: true,
				Sortable:   true,
				Validator: &schema.String{
					MaxLen: 150,
				},
			},
		},
	}

	// Define a post resource schema
	post = schema.Schema{
		Fields: schema.Fields{
			"id":      schema.IDField,
			"created": schema.CreatedField,
			"updated": schema.UpdatedField,
			"user": {
				Required:   true,
				Filterable: true,
				Validator: &schema.Reference{
					Path: "users",
				},
			},
			"public": {
				Filterable: true,
				Validator:  &schema.Bool{},
			},
			"meta": {
				Schema: &schema.Schema{
					Fields: schema.Fields{
						"title": {
							Required: true,
							Validator: &schema.String{
								MaxLen: 150,
							},
						},
						"body": {
							Validator: &schema.String{
								MaxLen: 100000,
							},
						},
					},
				},
			},
		},
	}
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("GOTEST_MONGODB")+"/exampledb"))
	db := "test_rest_layer"

	index := resource.NewIndex()

	users := index.Bind("users", user, mgo.NewHandler(client, db, "users"), resource.Conf{
		AllowedModes: resource.ReadWrite,
	})

	users.Bind("posts", "user", post, mgo.NewHandler(client, db, "posts"), resource.Conf{
		AllowedModes: resource.ReadWrite,
	})

	api, err := rest.NewHandler(index)
	if err != nil {
		log.Fatalf("Invalid API configuration: %s", err)
	}

	http.Handle("/", cors.New(cors.Options{OptionsPassthrough: true}).Handler(api))

	log.Print("Serving API on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
