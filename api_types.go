package modusdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/types"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

var (
	ErrNoObjFound  = fmt.Errorf("no object found")
	NoUniqueConstr = "unique constraint not defined for any field on type %s"
)

type UniqueField interface {
	uint64 | ConstrainedField
}
type ConstrainedField struct {
	Key   string
	Value any
}

type QueryParams struct {
	Filter     *Filter
	Pagination *Pagination
	Sorting    *Sorting
}

type Filter struct {
	Field  string
	String StringPredicate
	Vector VectorPredicate
	And    *Filter
	Or     *Filter
	Not    *Filter
}

type Pagination struct {
	Limit  int64
	Offset int64
	After  string
}

type Sorting struct {
	OrderAscField  string
	OrderDescField string
	OrderDescFirst bool
}

type StringPredicate struct {
	Equals         string
	LessThan       string
	LessOrEqual    string
	GreaterThan    string
	GreaterOrEqual string
	AllOfTerms     []string
	AnyOfTerms     []string
	AllOfText      []string
	AnyOfText      []string
	RegExp         string
}

type VectorPredicate struct {
	SimilarTo []float32
	TopK      int64
}

type ModusDbOption func(*modusDbOptions)

type modusDbOptions struct {
	namespace uint64
}

func WithNamespace(namespace uint64) ModusDbOption {
	return func(o *modusDbOptions) {
		o.namespace = namespace
	}
}

func getDefaultNamespace(db *DB, ns ...uint64) (context.Context, *Namespace, error) {
	dbOpts := &modusDbOptions{
		namespace: db.defaultNamespace.ID(),
	}
	for _, ns := range ns {
		WithNamespace(ns)(dbOpts)
	}

	n, err := db.getNamespaceWithLock(dbOpts.namespace)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	ctx = x.AttachNamespace(ctx, n.ID())

	return ctx, n, nil
}

func valueToPosting_ValType(v any) (pb.Posting_ValType, error) {
	switch v.(type) {
	case string:
		return pb.Posting_STRING, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32:
		return pb.Posting_INT, nil
	case uint64:
		return pb.Posting_UID, nil
	case bool:
		return pb.Posting_BOOL, nil
	case float32, float64:
		return pb.Posting_FLOAT, nil
	case []byte:
		return pb.Posting_BINARY, nil
	case time.Time:
		return pb.Posting_DATETIME, nil
	case geom.Point:
		return pb.Posting_GEO, nil
	case []float32, []float64:
		return pb.Posting_VFLOAT, nil
	default:
		return pb.Posting_DEFAULT, fmt.Errorf("unsupported type %T", v)
	}
}

func valueToApiVal(v any) (*api.Value, error) {
	switch val := v.(type) {
	case string:
		return &api.Value{Val: &api.Value_StrVal{StrVal: val}}, nil
	case int:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int8:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int16:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int32:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int64:
		return &api.Value{Val: &api.Value_IntVal{IntVal: val}}, nil
	case uint8:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint16:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint32:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint64:
		return &api.Value{Val: &api.Value_UidVal{UidVal: val}}, nil
	case bool:
		return &api.Value{Val: &api.Value_BoolVal{BoolVal: val}}, nil
	case float32:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: float64(val)}}, nil
	case float64:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: val}}, nil
	case []float32:
		return &api.Value{Val: &api.Value_Vfloat32Val{
			Vfloat32Val: types.FloatArrayAsBytes(val)}}, nil
	case []float64:
		float32Slice := make([]float32, len(val))
		for i, v := range val {
			float32Slice[i] = float32(v)
		}
		return &api.Value{Val: &api.Value_Vfloat32Val{
			Vfloat32Val: types.FloatArrayAsBytes(float32Slice)}}, nil
	case []byte:
		return &api.Value{Val: &api.Value_BytesVal{BytesVal: val}}, nil
	case time.Time:
		bytes, err := val.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return &api.Value{Val: &api.Value_DateVal{DateVal: bytes}}, nil
	case geom.Point:
		bytes, err := wkb.Marshal(&val, binary.LittleEndian)
		if err != nil {
			return nil, err
		}
		return &api.Value{Val: &api.Value_GeoVal{GeoVal: bytes}}, nil
	case uint:
		return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: fmt.Sprint(v)}}, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func filterToQueryFunc(typeName string, f Filter) QueryFunc {
	// Handle logical operators first
	if f.And != nil {
		return And(filterToQueryFunc(typeName, *f.And))
	}
	if f.Or != nil {
		return Or(filterToQueryFunc(typeName, *f.Or))
	}
	if f.Not != nil {
		return Not(filterToQueryFunc(typeName, *f.Not))
	}

	// Handle field predicates
	if f.String.Equals != "" {
		return buildEqQuery(getPredicateName(typeName, f.Field), f.String.Equals)
	}
	if len(f.String.AllOfTerms) != 0 {
		return buildAllOfTermsQuery(getPredicateName(typeName, f.Field), strings.Join(f.String.AllOfTerms, " "))
	}
	if len(f.String.AnyOfTerms) != 0 {
		return buildAnyOfTermsQuery(getPredicateName(typeName, f.Field), strings.Join(f.String.AnyOfTerms, " "))
	}
	if len(f.String.AllOfText) != 0 {
		return buildAllOfTextQuery(getPredicateName(typeName, f.Field), strings.Join(f.String.AllOfText, " "))
	}
	if len(f.String.AnyOfText) != 0 {
		return buildAnyOfTextQuery(getPredicateName(typeName, f.Field), strings.Join(f.String.AnyOfText, " "))
	}
	if f.String.RegExp != "" {
		return buildRegExpQuery(getPredicateName(typeName, f.Field), f.String.RegExp)
	}
	if f.String.LessThan != "" {
		return buildLtQuery(getPredicateName(typeName, f.Field), f.String.LessThan)
	}
	if f.String.LessOrEqual != "" {
		return buildLeQuery(getPredicateName(typeName, f.Field), f.String.LessOrEqual)
	}
	if f.String.GreaterThan != "" {
		return buildGtQuery(getPredicateName(typeName, f.Field), f.String.GreaterThan)
	}
	if f.String.GreaterOrEqual != "" {
		return buildGeQuery(getPredicateName(typeName, f.Field), f.String.GreaterOrEqual)
	}
	if f.Vector.SimilarTo != nil {
		return buildSimilarToQuery(getPredicateName(typeName, f.Field), f.Vector.TopK, f.Vector.SimilarTo)
	}

	// Return empty query if no conditions match
	return func() string { return "" }
}
