package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/clarify/rested/schema"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	// NewObjectID is a field hook handler that generates a new Mongo ObjectID hex if
	// value is nil to be used in schema with OnInit.
	NewObjectID = func(ctx context.Context, value interface{}) interface{} {
		if value == nil {
			value = primitive.NewObjectID().Hex()
		}
		return value
	}

	// ObjectIDField is a common schema field configuration that generate an Object ID
	// for new item id.
	ObjectIDField = schema.Field{
		Required:   true,
		ReadOnly:   true,
		OnInit:     NewObjectID,
		Filterable: true,
		Sortable:   true,
		Validator:  &ObjectID{},
	}
)

// ObjectID validates and serialize unique id
type ObjectID struct{}

// Validate implements FieldValidator interface
func (v ObjectID) Validate(value interface{}) (interface{}, error) {
	_, ok := value.(primitive.ObjectID)
	if ok {
		return value, nil
	}
	s, ok := value.(string)
	if !ok {
		return nil, errors.New("invalid object id")
	}
	if len(s) != 24 {
		return nil, errors.New("invalid object id length")
	}
	if id, err := primitive.ObjectIDFromHex(s); err != nil {
		return nil, fmt.Errorf("invalid object id")
	} else {
		return id, nil
	}
}

// Serialize implements FieldSerializer interface
func (v ObjectID) Serialize(value interface{}) (interface{}, error) {
	id, err := v.Validate(value)
	if err != nil {
		return nil, errors.New("not an ObjectId")
	}
	return id.(primitive.ObjectID).Hex(), nil
}

// BuildJSONSchema implements the jsonschema.Builder interface.
func (v ObjectID) BuildJSONSchema() (map[string]interface{}, error) {
	return map[string]interface{}{
		"type":    "string",
		"pattern": "^[0-9a-fA-F]{24}$",
	}, nil
}
