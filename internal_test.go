package modusgraph

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type AllTags struct {
	Name          string `json:"name,omitempty" dgraph:"index=exact upsert"`
	Email         string `json:"email,omitempty" dgraph:"index=hash unique upsert"`
	UserID        string `json:"user_id,omitempty" dgraph:"predicate=my_user_id index=hash unique upsert"`
	NoUpsert      string `json:"no_upsert,omitempty" dgraph:"index=term"`
	WithJson      string `json:"with_json" dgraph:"upsert"`
	WithNoTags    string
	PredicateOnly string   `json:"pred_only" dgraph:"predicate=pred_only upsert"`
	MultiTag      string   `json:"multi_tag" dgraph:"index=term,upsert ,unique"`
	UID           string   `json:"uid,omitempty"`
	EmployeeID    int      `json:"employee_id,omitempty" dgraph:"index=int unique"`
	DType         []string `json:"dgraph.type,omitempty"`
}

func TestStructUpsertTagsProcessing(t *testing.T) {

	tests := []struct {
		name      string
		input     any
		expected  []string
		firstOnly bool
	}{
		{
			"Single upsert field",
			struct {
				Name string `json:"name" dgraph:"upsert"`
			}{},
			[]string{"name"},
			false,
		},
		{
			"Index and upsert",
			struct {
				Name string `json:"name" dgraph:"index=exact upsert"`
			}{},
			[]string{"name"},
			false,
		},
		{
			"Predicate override",
			struct {
				UserID string `json:"user_id" dgraph:"predicate=my_user_id upsert"`
			}{},
			[]string{"my_user_id"},
			false,
		},
		{
			"Json fallback",
			struct {
				WithJson string `json:"with_json" dgraph:"upsert"`
			}{},
			[]string{"with_json"},
			false,
		},
		{
			"No upsert",
			struct {
				Desc string `json:"desc" dgraph:"index=term"`
			}{},
			[]string{},
			false,
		},
		{
			"Multiple upserts",
			struct {
				A string `json:"a" dgraph:"upsert"`
				B string `json:"b" dgraph:"upsert"`
			}{},
			[]string{"a", "b"},
			false,
		},
		{
			"MultiTag comma",
			struct {
				MultiTag string `json:"multi_tag" dgraph:"index=term,upsert,unique"`
			}{},
			[]string{"multi_tag"},
			false,
		},
		{
			"With format issues",
			struct {
				Count int `json:"count" dgraph:"index=int    upsert"`
			}{},
			[]string{"count"},
			false,
		},
		{
			"AllTags struct",
			AllTags{},
			[]string{"name", "email", "my_user_id", "with_json", "pred_only", "multi_tag"},
			false,
		},
		{
			"First only",
			struct {
				A string `json:"a" dgraph:"upsert"`
				B string `json:"b" dgraph:"upsert"`
			}{},
			[]string{"a"},
			true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			predMap := getUpsertPredicates(tc.input, tc.firstOnly)
			preds := make([]string, 0, len(predMap))
			for k := range predMap {
				preds = append(preds, k)
			}
			sort.Strings(preds)
			sort.Strings(tc.expected)
			require.Equal(t, tc.expected, preds)
		})
	}
}

func TestStructUniqueTagsProcessing(t *testing.T) {

	tests := []struct {
		name     string
		input    any
		expected []string
	}{
		{
			"Single unique field",
			struct {
				Name string `json:"name" dgraph:"unique"`
			}{},
			[]string{"name"},
		},
		{
			"Multiple unique fields",
			struct {
				Name  string `json:"name" dgraph:"unique"`
				Email string `json:"email" dgraph:"unique"`
			}{},
			[]string{"name", "email"},
		},
		{
			"No unique fields",
			struct {
				Name string `json:"name"`
			}{},
			[]string{},
		},
		{
			"AllTags struct",
			AllTags{},
			[]string{"email", "my_user_id", "multi_tag", "employee_id"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			predMap := getUniquePredicates(tc.input)
			preds := make([]string, 0, len(predMap))
			for k := range predMap {
				preds = append(preds, k)
			}
			sort.Strings(preds)
			sort.Strings(tc.expected)
			require.Equal(t, tc.expected, preds)
		})
	}
}

func TestGenerateFilterQuery(t *testing.T) {
	obj := &AllTags{
		Email:      "alice@example.com",
		UserID:     "123",
		EmployeeID: 123,
		MultiTag:   "multi_tag",
		DType:      []string{"User"},
	}
	predicates := getUniquePredicates(obj)
	query, vars := generateUniquePredicateQuery(predicates, obj.DType[0])

	require.True(t, strings.Index(query, "query q(") == 0)
	require.Contains(t, query, "$email: string")
	require.Contains(t, query, "$my_user_id: string")
	require.Contains(t, query, "$employee_id: int")
	require.Contains(t, query, "func: type(User)")
	require.Contains(t, query, "eq(email, $email)")
	require.Contains(t, query, "eq(my_user_id, $my_user_id)")
	require.Contains(t, query, "eq(employee_id, $employee_id)")
	require.Contains(t, query, "OR")
	require.Contains(t, query, "uid")

	require.Equal(t, "alice@example.com", vars["$email"])
	require.Equal(t, "123", vars["$my_user_id"])
	require.Equal(t, "123", vars["$employee_id"])
	require.Equal(t, "multi_tag", vars["$multi_tag"])

	//fmt.Println(query)
}
