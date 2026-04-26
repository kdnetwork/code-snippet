package utils

import "strings"

var NegativeValue = map[string]struct{}{
	"0": {}, "false": {}, "off": {}, "disabled": {}, "no": {}, "n": {},
}

var PositiveValue = map[string]struct{}{
	"1": {}, "true": {}, "on": {}, "enabled": {}, "yes": {}, "y": {},
}

func StrBoolF(value string) bool {
	if value == "" {
		return false
	}
	_, isNegative := NegativeValue[strings.ToLower(value)]
	return !isNegative
}

func StrBoolT(value string) bool {
	if value == "" {
		return false
	}
	_, isPositive := PositiveValue[strings.ToLower(value)]
	return isPositive
}
