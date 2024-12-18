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

func getFieldTags(t reflect.Type) (jsonTags map[string]string, jsonToDbTags map[string]*dbTag,
	reverseEdgeTags map[string]string, err error) {

	jsonTags = make(map[string]string)
	jsonToDbTags = make(map[string]*dbTag)
	reverseEdgeTags = make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			return nil, nil, nil, fmt.Errorf("field %s has no json tag", field.Name)
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		jsonTags[field.Name] = jsonName

		reverseEdgeTag := field.Tag.Get("readFrom")
		if reverseEdgeTag != "" {
			typeAndField := strings.Split(reverseEdgeTag, ",")
			if len(typeAndField) != 2 {
				return nil, nil, nil, fmt.Errorf(`field %s has invalid readFrom tag, 
				expected format is type=<type>,field=<field>`, field.Name)
			}
			t := strings.Split(typeAndField[0], "=")[1]
			f := strings.Split(typeAndField[1], "=")[1]
			reverseEdgeTags[field.Name] = getPredicateName(t, f)
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
	return jsonTags, jsonToDbTags, reverseEdgeTags, nil
}

func getFieldValues(object any, jsonFields map[string]string) map[string]any {
	values := make(map[string]any)
	v := reflect.ValueOf(object).Elem()
	for fieldName, jsonName := range jsonFields {
		fieldValue := v.FieldByName(fieldName)
		values[jsonName] = fieldValue.Interface()

	}
	return values
}

func createDynamicStruct(t reflect.Type, jsonFields map[string]string) reflect.Type {
	fields := make([]reflect.StructField, 0, len(jsonFields))
	for fieldName, jsonName := range jsonFields {
		field, _ := t.FieldByName(fieldName)
		if fieldName == "Gid" {
			fields = append(fields, reflect.StructField{
				Name: "Uid",
				Type: reflect.TypeOf(""),
				Tag:  reflect.StructTag(`json:"uid"`),
			})
		} else {
			fields = append(fields, reflect.StructField{
				Name: field.Name,
				Type: field.Type,
				Tag:  reflect.StructTag(fmt.Sprintf(`json:"%s.%s"`, t.Name(), jsonName)),
			})
		}
	}
	return reflect.StructOf(fields)
}

func mapDynamicToFinal(dynamic any, final any) {
	vFinal := reflect.ValueOf(final).Elem()
	vDynamic := reflect.ValueOf(dynamic).Elem()

	for i := 0; i < vDynamic.NumField(); i++ {
		field := vDynamic.Type().Field(i)
		value := vDynamic.Field(i)

		var finalField reflect.Value
		if field.Name == "Uid" {
			finalField = vFinal.FieldByName("Gid")
		} else {
			finalField = vFinal.FieldByName(field.Name)
		}
		if finalField.IsValid() && finalField.CanSet() {
			// if field name is uid, convert it to uint64
			if field.Name == "Uid" {
				uidStr := value.String()
    			uid, _ := strconv.ParseUint(uidStr, 0, 64)
				finalField.SetUint(uid)
			} else {
				finalField.Set(value)
			}
		}
	}
}
