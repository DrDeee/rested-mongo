package mongo

import (
	"errors"
	"fmt"

	"github.com/clarify/rested/schema"
)

// OptionalReference validates the ID of a linked resource.
type OptionalReference struct {
	Path            string
	validator       schema.FieldValidator
	SchemaValidator schema.Validator
}

// Compile validates v.Path against rc and stores the a FieldValidator for later use by v.Validate.
func (r *OptionalReference) Compile(rc schema.ReferenceChecker) error {
	if rc == nil {
		return fmt.Errorf("rc can not be nil")
	}

	if v, sv := rc.ReferenceChecker(r.Path); v != nil && sv != nil {
		r.validator = v
		r.SchemaValidator = sv
		return nil
	}

	return fmt.Errorf("can't find resource '%s'", r.Path)
}

// Validate validates and sanitizes IDs against the reference path.
func (r OptionalReference) Validate(value interface{}) (interface{}, error) {
	if r.validator == nil {
		return nil, errors.New("not successfully compiled")
	}
	if value == nil {
		return nil, nil
	}

	return r.validator.Validate(value)
}

// GetField implements the FieldGetter interface.
func (r OptionalReference) GetField(name string) *schema.Field {
	return r.SchemaValidator.GetField(name)
}
