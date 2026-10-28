package messageprotocol

import "fmt"

func GetKeyAsString(m map[string]any, key string) string {
	val, ok := m[key]
	if !ok || val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

func GetKeyAsInt64(m map[string]any, key string) int64 {
	val, ok := m[key]
	if !ok || val == nil {
		return 0
	}
	switch v := val.(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func GetSubMap(m map[string]any, key string) map[string]interface{} {
	val, ok := m[key]
	if !ok || val == nil {
		return nil
	}
	subMap, ok := val.(map[string]interface{})
	if !ok {
		return nil
	}
	return subMap
}
