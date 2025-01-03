package utils

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type DbTag struct {
	Constraint string
}

func GetFieldTags(t reflect.Type) (fieldToJsonTags map[string]string,
	jsonToDbTags map[string]*DbTag, jsonToReverseEdgeTags map[string]string, err error) {

	fieldToJsonTags = make(map[string]string)
	jsonToDbTags = make(map[string]*DbTag)
	jsonToReverseEdgeTags = make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			return nil, nil, nil, fmt.Errorf("field %s has no json tag", field.Name)
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		fieldToJsonTags[field.Name] = jsonName

		reverseEdgeTag := field.Tag.Get("readFrom")
		if reverseEdgeTag != "" {
			typeAndField := strings.Split(reverseEdgeTag, ",")
			if len(typeAndField) != 2 {
				return nil, nil, nil, fmt.Errorf(`field %s has invalid readFrom tag, 
				expected format is type=<type>,field=<field>`, field.Name)
			}
			t := strings.Split(typeAndField[0], "=")[1]
			f := strings.Split(typeAndField[1], "=")[1]
			jsonToReverseEdgeTags[jsonName] = GetPredicateName(t, f)
		}

		dbConstraintsTag := field.Tag.Get("db")
		if dbConstraintsTag != "" {
			jsonToDbTags[jsonName] = &DbTag{}
			dbTagsSplit := strings.Split(dbConstraintsTag, ",")
			for _, dbTag := range dbTagsSplit {
				split := strings.Split(dbTag, "=")
				if split[0] == "constraint" {
					jsonToDbTags[jsonName].Constraint = split[1]
				}
			}
		}
	}
	return fieldToJsonTags, jsonToDbTags, jsonToReverseEdgeTags, nil
}

func GetJsonTagToValues(object any, fieldToJsonTags map[string]string) map[string]any {
	values := make(map[string]any)
	v := reflect.ValueOf(object)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for fieldName, jsonName := range fieldToJsonTags {
		fieldValue := v.FieldByName(fieldName)
		values[jsonName] = fieldValue.Interface()

	}
	return values
}

func CreateDynamicStruct(t reflect.Type, fieldToJsonTags map[string]string, depth int) reflect.Type {
	fields := make([]reflect.StructField, 0, len(fieldToJsonTags))
	for fieldName, jsonName := range fieldToJsonTags {
		field, _ := t.FieldByName(fieldName)
		if fieldName != "Gid" {
			if field.Type.Kind() == reflect.Struct {
				if depth <= 1 {
					nestedFieldToJsonTags, _, _, _ := GetFieldTags(field.Type)
					nestedType := CreateDynamicStruct(field.Type, nestedFieldToJsonTags, depth+1)
					fields = append(fields, reflect.StructField{
						Name: field.Name,
						Type: nestedType,
						Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
					})
				}
			} else if field.Type.Kind() == reflect.Ptr &&
				field.Type.Elem().Kind() == reflect.Struct {
				nestedFieldToJsonTags, _, _, _ := GetFieldTags(field.Type.Elem())
				nestedType := CreateDynamicStruct(field.Type.Elem(), nestedFieldToJsonTags, depth+1)
				fields = append(fields, reflect.StructField{
					Name: field.Name,
					Type: reflect.PointerTo(nestedType),
					Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
				})
			} else if field.Type.Kind() == reflect.Slice &&
				field.Type.Elem().Kind() == reflect.Struct {
				nestedFieldToJsonTags, _, _, _ := GetFieldTags(field.Type.Elem())
				nestedType := CreateDynamicStruct(field.Type.Elem(), nestedFieldToJsonTags, depth+1)
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
						return 0, ErrNoObjFound
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
		if dynamicFieldType.Kind() == reflect.Struct {
			_, err := MapDynamicToFinal(dynamicValue.Addr().Interface(), finalField.Addr().Interface(), true)
			if err != nil {
				return 0, err
			}
		} else if dynamicFieldType.Kind() == reflect.Ptr &&
			dynamicFieldType.Elem().Kind() == reflect.Struct {
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
