package modusdb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type dbTag struct {
	constraint string
}

func getFieldTags(t reflect.Type) (fieldToJsonTags map[string]string,
	jsonToDbTags map[string]*dbTag, jsonToReverseEdgeTags map[string]string, err error) {

	fieldToJsonTags = make(map[string]string)
	jsonToDbTags = make(map[string]*dbTag)
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
			jsonToReverseEdgeTags[jsonName] = getPredicateName(t, f)
		}

		dbConstraintsTag := field.Tag.Get("db")
		if dbConstraintsTag != "" {
			jsonToDbTags[jsonName] = &dbTag{}
			dbTagsSplit := strings.Split(dbConstraintsTag, ",")
			for _, dbTag := range dbTagsSplit {
				split := strings.Split(dbTag, "=")
				if split[0] == "constraint" {
					jsonToDbTags[jsonName].constraint = split[1]
				}
			}
		}
	}
	return fieldToJsonTags, jsonToDbTags, jsonToReverseEdgeTags, nil
}

func getJsonTagToValues(object any, fieldToJsonTags map[string]string) map[string]any {
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

func createDynamicStruct(t reflect.Type, fieldToJsonTags map[string]string, depth int) reflect.Type {
	fields := make([]reflect.StructField, 0, len(fieldToJsonTags))
	for fieldName, jsonName := range fieldToJsonTags {
		field, _ := t.FieldByName(fieldName)
		if fieldName != "Gid" {
			if field.Type.Kind() == reflect.Struct {
				if depth <= 2 {
					nestedFieldToJsonTags, _, _, _ := getFieldTags(field.Type)
					nestedType := createDynamicStruct(field.Type, nestedFieldToJsonTags, depth+1)
					fields = append(fields, reflect.StructField{
						Name: field.Name,
						Type: nestedType,
						Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
					})
				}
			} else if field.Type.Kind() == reflect.Ptr &&
				field.Type.Elem().Kind() == reflect.Struct {
				nestedFieldToJsonTags, _, _, _ := getFieldTags(field.Type.Elem())
				nestedType := createDynamicStruct(field.Type.Elem(), nestedFieldToJsonTags, depth+1)
				fields = append(fields, reflect.StructField{
					Name: field.Name,
					Type: reflect.PointerTo(nestedType),
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
		Name: "Uid",
		Type: reflect.TypeOf(""),
		Tag:  reflect.StructTag(`json:"uid"`),
	}, reflect.StructField{
		Name: "DgraphType",
		Type: reflect.TypeOf([]string{}),
		Tag:  reflect.StructTag(`json:"dgraph.type"`),
	})
	return reflect.StructOf(fields)
}

func mapDynamicToFinal(dynamic any, final any) (uint64, error) {
	vFinal := reflect.ValueOf(final).Elem()
	vDynamic := reflect.ValueOf(dynamic).Elem()

	gid := uint64(0)

	for i := 0; i < vDynamic.NumField(); i++ {

		dynamicField := vDynamic.Type().Field(i)
		dynamicFieldType := dynamicField.Type
		dynamicValue := vDynamic.Field(i)

		var finalField reflect.Value
		if dynamicField.Name == "Uid" {
			finalField = vFinal.FieldByName("Gid")
			gidStr := dynamicValue.String()
			gid, _ = strconv.ParseUint(gidStr, 0, 64)
		} else if dynamicField.Name == "DgraphType" {
			fieldArr := dynamicValue.Interface().([]string)
			if len(fieldArr) == 0 {
				return 0, ErrNoObjFound
			}
		} else {
			finalField = vFinal.FieldByName(dynamicField.Name)
		}
		if dynamicFieldType.Kind() == reflect.Struct {
			_, err := mapDynamicToFinal(dynamicValue.Addr().Interface(), finalField.Addr().Interface())
			if err != nil {
				return 0, err
			}
		} else if dynamicFieldType.Kind() == reflect.Ptr &&
			dynamicFieldType.Elem().Kind() == reflect.Struct {
			// if field is a pointer, find if the underlying is a struct
			_, err := mapDynamicToFinal(dynamicValue.Interface(), finalField.Interface())
			if err != nil {
				return 0, err
			}

		} else {
			if finalField.IsValid() && finalField.CanSet() {
				// if field name is uid, convert it to uint64
				if dynamicField.Name == "Uid" {
					finalField.SetUint(gid)
				} else {
					finalField.Set(dynamicValue)
				}
			}
		}
	}
	return gid, nil
}

func getUniqueConstraint[T any](object T) (uint64, *ConstrainedField, error) {
	t := reflect.TypeOf(object)
	fieldToJsonTags, jsonToDbTags, _, err := getFieldTags(t)
	if err != nil {
		return 0, nil, err
	}
	jsonTagToValue := getJsonTagToValues(object, fieldToJsonTags)

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
		if jsonToDbTags[jsonName] != nil && isValidUniqueIndex(jsonToDbTags[jsonName].constraint) {
			// check if value is zero or nil
			if value == reflect.Zero(reflect.TypeOf(value)).Interface() || value == nil {
				continue
			}
			return 0, &ConstrainedField{
				Key:   jsonName,
				Value: value,
			}, nil
		}
	}

	return 0, nil, fmt.Errorf(NoUniqueConstr, t.Name())
}

func isValidUniqueIndex(name string) bool {
	return name == "unique"
}
