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

func GetBoolEnv(key string) bool {
	return StrBoolF(GetEnv(key, ""))
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
