// Package mongo is a REST Layer resource storage handler for MongoDB using mgo
package mongo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/clarify/rested/resource"
	"github.com/clarify/rested/schema/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// mongoItem is a bson representation of a resource.Item.
type mongoItem struct {
	ID      interface{}            `bson:"_id"`
	ETag    string                 `bson:"_etag"`
	Updated time.Time              `bson:"_updated"`
	Payload map[string]interface{} `bson:",inline"`
}

// newMongoItem converts a resource.Item into a mongoItem.
func newMongoItem(i *resource.Item) *mongoItem {
	// Filter out id from the payload so we don't store it twice
	p := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			p[k] = v
		}
	}
	return &mongoItem{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: p,
	}
}

// newItem converts a back mongoItem into a resource.Item.
func newItem(i *mongoItem) *resource.Item {
	// If there is no field except those defined in mongoItem, Payload could be nil
	// when just fetched from the database.
	if i.Payload == nil {
		i.Payload = make(map[string]interface{})
	}
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	i.Payload["id"] = i.ID
	item := &resource.Item{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: i.Payload,
	}

	if item.ETag == "" {
		if v, ok := i.ID.(primitive.ObjectID); ok {
			item.ETag = "p-" + v.Hex()
		} else {
			item.ETag = "p-" + fmt.Sprint(i.ID)
		}
	}
	return item
}

// Handler handles resource storage in a MongoDB collection.
type Handler func(ctx context.Context) (*mongo.Collection, error)

// NewHandler creates an new mongo handler
func NewHandler(c *mongo.Client, db, collectionName string) Handler {
	collection := func() *mongo.Collection {
		return c.Database(db).Collection(collectionName)
	}
	return func(ctx context.Context) (*mongo.Collection, error) {
		return collection(), nil
	}
}

// Insert inserts new items in the mongo collection.
func (m Handler) Insert(ctx context.Context, items []*resource.Item) error {
	mItems := make([]interface{}, len(items))
	for i, item := range items {
		mItems[i] = newMongoItem(item)
	}
	c, err := m(ctx)
	if err != nil {
		return err
	}
	_, err = c.InsertMany(ctx, mItems)
	if mongo.IsDuplicateKeyError(err) {
		// Duplicate ID key
		err = resource.ErrConflict
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Update replace an item by a new one in the mongo collection.
func (m Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	mItem := newMongoItem(item)
	c, err := m(ctx)
	if err != nil {
		return err
	}
	s := bson.M{"_id": original.ID}
	if strings.HasPrefix(original.ETag, "p-") {
		// If the original ETag is in "p-[id]" format,
		// then _etag field must be absent from the resource in DB
		s["_etag"] = bson.M{"$exists": false}
	} else {
		s["_etag"] = original.ETag
	}
	_, err = c.UpdateByID(ctx, original.ID, bson.M{
		"$set": mItem,
	})
	if err == mongo.ErrNoDocuments {
		// Determine if the item is not found or if the item is found but etag missmatch
		var object interface{}
		result := c.FindOne(ctx, bson.M{"_id": original.ID})
		err := result.Decode(object)
		if err == mongo.ErrNoDocuments {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
		return err
	}
	return err
}

// Delete deletes an item from the mongo collection.
func (m Handler) Delete(ctx context.Context, item *resource.Item) error {
	c, err := m(ctx)
	if err != nil {
		return err
	}
	s := bson.M{"_id": item.ID}
	if strings.HasPrefix(item.ETag, "p-") {
		// If the item ETag is in "p-[id]" format,
		// then _etag field must be absent from the resource in DB
		s["_etag"] = bson.M{"$exists": false}
	} else {
		s["_etag"] = item.ETag
	}
	_, err = c.DeleteOne(ctx, s)
	if err == mongo.ErrNoDocuments {
		var object interface{}
		result := c.FindOne(ctx, bson.M{"_id": item.ID})
		err := result.Decode(object)
		if err == mongo.ErrNoDocuments {
			return resource.ErrNotFound
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			return resource.ErrConflict
		}
	}
	return err
}

// Clear clears all items from the mongo collection matching the query. Note
// that when q.Window != nil, the current implementation may error if the BSON
// encoding of all matching IDs according to the q.Window length gets close to
// the maximum document size in MongDB (usually 16MiB):
// https://docs.mongodb.com/manual/reference/limits/#bson-documents
func (m Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	// When not applying windowing, qry will be passed directly to RemoveAll.
	qry, err := getQuery(q)
	if err != nil {
		return 0, err
	}

	c, err := m(ctx)
	if err != nil {
		return 0, err
	}

	if q.Window != nil {
		// RemoveAll does not allow skip and limit to be set. To workaround
		// this we do an additional pre-query to retrieve a sorted and sliced
		// list of the IDs for all items to be deleted.
		//
		// This solution does not handle the case where a query containg all
		// IDs is larger than the maximum BSON document size in MongoDB:
		// https://docs.mongodb.com/manual/reference/limits/#bson-documents
		options := getSort(q)
		options = applyWindow(options, q.Window)
		options = options.SetProjection(bson.M{"_id": 1})

		request, err := c.Find(ctx, qry, options)
		if err != nil {
			return 0, err
		}

		if ids, err := selectIDs(ctx, request); err == nil {
			qry = bson.M{"_id": bson.M{"$in": ids}}
		} else {
			return 0, err
		}
	}

	// We handle the potential of partial failure by returning both the number
	// of removed items and an error, if both are present.
	info, err := c.DeleteMany(ctx, qry)
	if err == nil {
		err = ctx.Err()
	}
	if info == nil {
		return 0, err
	}
	return int(info.DeletedCount), err
}

// Find items from the mongo collection matching the provided query.
func (m Handler) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	// MongoDB will return all records on Limit=0. Workaround that behavior.
	// https://docs.mongodb.com/manual/reference/method/cursor.limit/#zero-value
	if q.Window != nil && q.Window.Limit == 0 {
		n, err := m.Count(ctx, q)
		if err != nil {
			return nil, err
		}
		list := &resource.ItemList{
			Total: n,
			Limit: q.Window.Limit,
			Items: []*resource.Item{},
		}
		return list, err
	}

	qry, err := getQuery(q)
	if err != nil {
		return nil, err
	}
	srt := getSort(q)
	srt = applyWindow(srt, q.Window)

	c, err := m(ctx)
	if err != nil {
		return nil, err
	}

	mq, err := c.Find(ctx, qry, srt)
	limit := -1
	if q.Window != nil {
		limit = q.Window.Limit
	}

	// Total is set to -1 because we have no easy way with MongoDB to to compute
	// this value without performing two requests.
	list := &resource.ItemList{
		Total: -1,
		Limit: limit,
		Items: []*resource.Item{},
	}

	var items []mongoItem
	mq.All(ctx, &items)
	for _, item := range items {
		list.Items = append(list.Items, newItem(&item))
	}

	if err := mq.Close(ctx); err != nil {
		return nil, err
	}
	// If the number of returned elements is lower than requested limit, or no
	// limit is requested, we can deduce the total number of element for free.
	if limit < 0 || len(list.Items) < limit {
		if q.Window != nil && q.Window.Offset > 0 {
			if len(list.Items) > 0 {
				list.Total = q.Window.Offset + len(list.Items)
			}
			// If there are no items returned when Offset > 0, we may be out-of-bounds,
			// and therefore cannot deduce the total count of items.
		} else {
			list.Total = len(list.Items)
		}
	}
	return list, err
}

// Count counts the number items matching the lookup filter
func (m Handler) Count(ctx context.Context, query *query.Query) (int, error) {
	q, err := getQuery(query)
	if err != nil {
		return -1, err
	}
	c, err := m(ctx)
	if err != nil {
		return -1, err
	}
	cnt, err := c.CountDocuments(ctx, q)
	return int(cnt), err
}
