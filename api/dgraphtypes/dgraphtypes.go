/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package dgraphtypes

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/hypermodeinc/dgraph/v25/protos/pb"
	"github.com/hypermodeinc/dgraph/v25/types"
	"github.com/pkg/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"

	modusapi "github.com/hypermodeinc/modusgraph/api"
	"github.com/hypermodeinc/modusgraph/api/structreflect"
)

func addIndex(u *pb.SchemaUpdate, index string, uniqueConstraintExists bool) bool {
	u.Directive = pb.SchemaUpdate_INDEX
	switch index {
	case "exact":
		u.Tokenizer = []string{"exact"}
	case "term":
		u.Tokenizer = []string{"term"}
	case "hash":
		u.Tokenizer = []string{"hash"}
	case "unique":
		u.Tokenizer = []string{"exact"}
		u.Unique = true
		u.Upsert = true
		uniqueConstraintExists = true
	case "fulltext":
		u.Tokenizer = []string{"fulltext"}
	case "trigram":
		u.Tokenizer = []string{"trigram"}
	case "vector":
		u.IndexSpecs = []*pb.VectorIndexSpec{
			{
				Name: "hnsw",
				Options: []*pb.OptionPair{
					{
						Key:   "metric",
						Value: "cosine",
					},
				},
			},
		}
	default:
		return uniqueConstraintExists
	}
	return uniqueConstraintExists
}

func ValueToPosting_ValType(v any) (pb.Posting_ValType, error) {
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
	case modusapi.Point, modusapi.Polygon:
		return pb.Posting_GEO, nil
	case []float32, []float64:
		return pb.Posting_VFLOAT, nil
	default:
		return pb.Posting_DEFAULT, fmt.Errorf("value to posting, unsupported type %T", v)
	}
}

// ValueToApiVal converts a value to an api.Value. Note the result can be nil for empty non-scalar types
func ValueToApiVal(v any) (*api.Value, error) {
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
		return &api.Value{Val: &api.Value_DatetimeVal{DatetimeVal: bytes}}, nil
	case modusapi.Point:
		if len(val.Coordinates) == 0 {
			return nil, nil
		}
		point, err := geom.NewPoint(geom.XY).SetCoords(val.Coordinates)
		if err != nil {
			return nil, errors.Wrap(err, "converting point to api value")
		}
		bytes, err := wkb.Marshal(point, binary.LittleEndian)
		if err != nil {
			return nil, errors.Wrap(err, "marshalling point to wkb")
		}
		return &api.Value{Val: &api.Value_GeoVal{GeoVal: bytes}}, nil
	case modusapi.Polygon:
		if len(val.Coordinates) == 0 {
			return nil, nil
		}
		coords := make([][]geom.Coord, len(val.Coordinates))
		for i, polygon := range val.Coordinates {
			coords[i] = make([]geom.Coord, len(polygon))
			for j, point := range polygon {
				coords[i][j] = geom.Coord{point[0], point[1]}
			}
		}
		polygon, err := geom.NewPolygon(geom.XY).SetCoords(coords)
		if err != nil {
			return nil, errors.Wrap(err, "converting polygon to api value")
		}
		bytes, err := wkb.Marshal(polygon, binary.LittleEndian)
		if err != nil {
			return nil, errors.Wrap(err, "marshalling polygon to wkb")
		}
		return &api.Value{Val: &api.Value_GeoVal{GeoVal: bytes}}, nil
	case uint:
		return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: fmt.Sprint(v)}}, nil
	default:
		return nil, fmt.Errorf("convert value to api value, unsupported type %T", v)
	}
}

func HandleConstraints(u *pb.SchemaUpdate, jsonToDbTags map[string]*structreflect.DbTag, jsonName string,
	valType pb.Posting_ValType, uniqueConstraintFound bool) (bool, error) {
	if jsonToDbTags[jsonName] == nil {
		return uniqueConstraintFound, nil
	}

	constraint := jsonToDbTags[jsonName].Constraint
	if constraint == "vector" && valType != pb.Posting_VFLOAT {
		return false, fmt.Errorf("vector index can only be applied to []float values")
	}

	return addIndex(u, constraint, uniqueConstraintFound), nil
}
