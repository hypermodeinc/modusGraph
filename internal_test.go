package modusgraph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStructTagsProcessing(t *testing.T) {
	type AllTags struct {
		Name          string `json:"name,omitempty" dgraph:"index=exact upsert"`
		Email         string `json:"email,omitempty" dgraph:"index=hash unique upsert"`
		UserID        string `json:"user_id,omitempty" dgraph:"predicate=my_user_id index=hash upsert"`
		NoUpsert      string `json:"no_upsert,omitempty" dgraph:"index=term"`
		WithJson      string `json:"with_json" dgraph:"upsert"`
		WithNoTags    string
		PredicateOnly string   `json:"pred_only" dgraph:"predicate=pred_only upsert"`
		MultiTag      string   `json:"multi_tag" dgraph:"index=term,upsert,unique"`
		UID           string   `json:"uid,omitempty"`
		DType         []string `json:"dgraph.type,omitempty"`
	}

	tests := []struct {
		name     string
		input    any
		expected []string
	}{
		{
			"Single upsert field",
			struct {
				Name string `json:"name" dgraph:"upsert"`
			}{},
			[]string{"name"},
		},
		{
			"Index and upsert",
			struct {
				Name string `json:"name" dgraph:"index=exact upsert"`
			}{},
			[]string{"name"},
		},
		{
			"Predicate override",
			struct {
				UserID string `json:"user_id" dgraph:"predicate=my_user_id upsert"`
			}{},
			[]string{"my_user_id"},
		},
		{
			"Json fallback",
			struct {
				WithJson string `json:"with_json" dgraph:"upsert"`
			}{},
			[]string{"with_json"},
		},
		{
			"No upsert",
			struct {
				Desc string `json:"desc" dgraph:"index=term"`
			}{},
			[]string{},
		},
		{
			"Multiple upserts",
			struct {
				A string `json:"a" dgraph:"upsert"`
				B string `json:"b" dgraph:"upsert"`
			}{},
			[]string{"a", "b"},
		},
		{
			"MultiTag comma",
			struct {
				MultiTag string `json:"multi_tag" dgraph:"index=term,upsert,unique"`
			}{},
			[]string{"multi_tag"},
		},
		{
			"With format issues",
			struct {
				Count int `json:"count" dgraph:"index=int    upsert"`
			}{},
			[]string{"count"},
		},
		{
			"AllTags struct",
			AllTags{},
			[]string{"name", "email", "my_user_id", "with_json", "pred_only", "multi_tag"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			preds := getUpsertPredicates(tc.input)
			require.Equal(t, tc.expected, preds)
		})
	}
}
