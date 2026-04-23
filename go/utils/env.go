package utils

import (
	"os"
	"strconv"
)

func GetEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

var envFalseValues = map[string]struct{}{
	"0": {}, "false": {}, "off": {}, "disabled": {}, "no": {}, "n": {},
}

func GetBoolEnv(key string) bool {
	var envVal = GetEnv(key, "")
	_, isFalse := envFalseValues[envVal]
	return !(envVal == "" || isFalse)
}

func GetIntEnv(key string, defaultValue int) int {
	var envVal = GetEnv(key, "")
	if envVal == "" {
		return defaultValue
	}

	val, err := strconv.Atoi(envVal)
	if err != nil {
		return defaultValue
	}

	return val
}
