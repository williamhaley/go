package api

import "reflect"

func ToApiData(data interface{}) interface{} {
	v := reflect.ValueOf(data)

	switch reflect.TypeOf(data).Kind() {
	case reflect.Ptr:
		v = reflect.ValueOf(data).Elem()
	case reflect.Slice:
		s := reflect.ValueOf(data)
		asSlice := make([]interface{}, s.Len())
		for index := 0; index < s.Len(); index++ {
			asSlice[index] = ToApiData(s.Index(index).Interface())
		}
		return asSlice
	default:
		return data
	}

	numberOfFields := v.NumField()

	res := make(map[string]interface{}, numberOfFields)

	for i := 0; i < numberOfFields; i++ {
		fieldInfo := v.Type().Field(i)

		if fieldInfo.Tag.Get("api") == "exclude" {
			continue
		}

		fieldName := fieldInfo.Name
		if fieldInfo.Tag.Get("json") != "" {
			fieldName = fieldInfo.Tag.Get("json")
		}

		fieldData := v.Field(i).Interface()

		switch v.Field(i).Kind() {
		case reflect.Struct, reflect.Ptr:
			res[fieldName] = ToApiData(fieldData)
		case reflect.Slice:
			s := reflect.ValueOf(fieldData)
			asSlice := make([]interface{}, s.Len())
			for index := 0; index < s.Len(); index++ {
				asSlice[index] = ToApiData(s.Index(index).Interface())
			}
			res[fieldName] = asSlice
		default:
			res[fieldName] = fieldData
		}
	}

	return res
}
