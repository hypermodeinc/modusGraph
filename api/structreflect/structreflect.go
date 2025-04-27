/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package structreflect

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/hypermodeinc/modusdb/api"
	"github.com/hypermodeinc/modusdb/api/apiutils"
)

func GetFieldTags(t reflect.Type) (*TagMaps, error) {
	tags := &TagMaps{
		FieldToJson:       make(map[string]string),
		JsonToDb:          make(map[string]*DbTag),
		JsonToReverseEdge: make(map[string]string),
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		jsonName, err := parseJsonTag(field)
		if err != nil {
			return nil, err
		}
		tags.FieldToJson[field.Name] = jsonName

		if reverseEdge, err := parseReverseEdgeTag(field); err != nil {
			return nil, err
		} else if reverseEdge != "" {
			tags.JsonToReverseEdge[jsonName] = reverseEdge
		}

		if dbTag := parseDbTag(field); dbTag != nil {
			tags.JsonToDb[jsonName] = dbTag
		}
	}

	return tags, nil
}

var skipProcessStructTypes = []reflect.Type{
	reflect.TypeOf(api.Point{}),
	reflect.TypeOf(api.Polygon{}),
	reflect.TypeOf(api.MultiPolygon{}),
	reflect.TypeOf(time.Time{}),
}

func IsDgraphType(value any) bool {
	valueType := reflect.TypeOf(value)
	if valueType.Kind() == reflect.Ptr {
		valueType = valueType.Elem()
	}
	for _, t := range skipProcessStructTypes {
		if valueType == t {
			return true
		}
	}
	return false
}

func IsStructAndNotDgraphType(field reflect.StructField) bool {
	fieldType := field.Type
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}
	if fieldType.Kind() != reflect.Struct {
		return false
	}
	for _, t := range skipProcessStructTypes {
		if fieldType == t {
			return false
		}
	}
	return true
}

func CreateDynamicStruct(t reflect.Type, fieldToJson map[string]string, depth int) reflect.Type {
	fields := make([]reflect.StructField, 0, len(fieldToJson))
	for fieldName, jsonName := range fieldToJson {
		field, _ := t.FieldByName(fieldName)
		if fieldName != "Gid" {
			if IsStructAndNotDgraphType(field) {
				if depth <= 1 {
					tagMaps, _ := GetFieldTags(field.Type)
					nestedType := CreateDynamicStruct(field.Type, tagMaps.FieldToJson, depth+1)
					fields = append(fields, reflect.StructField{
						Name: field.Name,
						Type: nestedType,
						Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
					})
				}
			} else if field.Type.Kind() == reflect.Ptr &&
				IsStructAndNotDgraphType(field) {
				tagMaps, _ := GetFieldTags(field.Type.Elem())
				nestedType := CreateDynamicStruct(field.Type.Elem(), tagMaps.FieldToJson, depth+1)
				fields = append(fields, reflect.StructField{
					Name: field.Name,
					Type: reflect.PointerTo(nestedType),
					Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
				})
			} else if field.Type.Kind() == reflect.Slice &&
				field.Type.Elem().Kind() == reflect.Struct {
				tagMaps, _ := GetFieldTags(field.Type.Elem())
				nestedType := CreateDynamicStruct(field.Type.Elem(), tagMaps.FieldToJson, depth+1)
				fields = append(fields, reflect.StructField{
					Name: field.Name,
					Type: reflect.SliceOf(nestedType),
					Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
				})
			} else {
				fields = append(fields, reflect.StructField{
					Name: field.Name,
					Type: field.Type,
					Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
				})
			}

		}
	}
	fields = append(fields, reflect.StructField{
		Name: "Gid",
		Type: reflect.TypeOf(""),
		Tag:  reflect.StructTag(`json:"gid"`),
	}, reflect.StructField{
		Name: "DgraphType",
		Type: reflect.TypeOf([]string{}),
		Tag:  reflect.StructTag(`json:"dgraph.type"`),
	})
	return reflect.StructOf(fields)
}

func MapDynamicToFinal(dynamic any, final any, isNested bool) (uint64, error) {
	vFinal := reflect.ValueOf(final).Elem()
	vDynamic := reflect.ValueOf(dynamic).Elem()

	gid := uint64(0)

	for i := 0; i < vDynamic.NumField(); i++ {

		dynamicField := vDynamic.Type().Field(i)
		dynamicFieldType := dynamicField.Type
		dynamicValue := vDynamic.Field(i)

		var finalField reflect.Value
		if dynamicField.Name == "Gid" {
			finalField = vFinal.FieldByName("Gid")
			gidStr := dynamicValue.String()
			gid, _ = strconv.ParseUint(gidStr, 0, 64)
		} else if dynamicField.Name == "DgraphType" {
			fieldArrInterface := dynamicValue.Interface()
			fieldArr, ok := fieldArrInterface.([]string)
			if ok {
				if len(fieldArr) == 0 {
					if !isNested {
						return 0, apiutils.ErrNoObjFound
					} else {
						continue
					}
				}
			} else {
				return 0, fmt.Errorf("DgraphType field should be an array of strings")
			}
		} else {
			finalField = vFinal.FieldByName(dynamicField.Name)
		}
		//if dynamicFieldType.Kind() == reflect.Struct {
		if IsStructAndNotDgraphType(dynamicField) {
			_, err := MapDynamicToFinal(dynamicValue.Addr().Interface(), finalField.Addr().Interface(), true)
			if err != nil {
				return 0, err
			}
		} else if dynamicFieldType.Kind() == reflect.Ptr &&
			IsStructAndNotDgraphType(dynamicField) {
			// if field is a pointer, find if the underlying is a struct
			_, err := MapDynamicToFinal(dynamicValue.Interface(), finalField.Interface(), true)
			if err != nil {
				return 0, err
			}
		} else if dynamicFieldType.Kind() == reflect.Slice &&
			dynamicFieldType.Elem().Kind() == reflect.Struct {
			for j := 0; j < dynamicValue.Len(); j++ {
				sliceElem := dynamicValue.Index(j).Addr().Interface()
				finalSliceElem := reflect.New(finalField.Type().Elem()).Elem()
				_, err := MapDynamicToFinal(sliceElem, finalSliceElem.Addr().Interface(), true)
				if err != nil {
					return 0, err
				}
				finalField.Set(reflect.Append(finalField, finalSliceElem))
			}
		} else {
			if finalField.IsValid() && finalField.CanSet() {
				// if field name is gid, convert it to uint64
				if dynamicField.Name == "Gid" {
					finalField.SetUint(gid)
				} else {
					finalField.Set(dynamicValue)
				}
			}
		}
	}
	return gid, nil
}

func ConvertDynamicToTyped[T any](obj any, t reflect.Type) (uint64, T, error) {
	var result T
	finalObject := reflect.New(t).Interface()
	gid, err := MapDynamicToFinal(obj, finalObject, false)
	if err != nil {
		return 0, result, err
	}

	if typedPtr, ok := finalObject.(*T); ok {
		return gid, *typedPtr, nil
	} else if dirType, ok := finalObject.(T); ok {
		return gid, dirType, nil
	}
	return 0, result, fmt.Errorf("failed to convert type %T to %T", finalObject, obj)
}

func GetUniqueConstraint[T any](object T) (uint64, *keyValue, error) {
	t := reflect.TypeOf(object)
	tagMaps, err := GetFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	jsonTagToValue := GetJsonTagToValues(object, tagMaps.FieldToJson)

	for jsonName, value := range jsonTagToValue {
		if jsonName == "gid" {
			gid, ok := value.(uint64)
			if !ok {
				continue
			}
			if gid != 0 {
				return gid, nil, nil
			}
		}
		if tagMaps.JsonToDb[jsonName] != nil && IsValidUniqueIndex(tagMaps.JsonToDb[jsonName].Constraint) {
			// check if value is zero or nil
			if value == reflect.Zero(reflect.TypeOf(value)).Interface() || value == nil {
				continue
			}
			return 0, &keyValue{key: jsonName, value: value}, nil
		}
	}

	return 0, nil, fmt.Errorf(apiutils.NoUniqueConstr, t.Name())
}

func IsValidUniqueIndex(name string) bool {
	return name == "unique"
}
