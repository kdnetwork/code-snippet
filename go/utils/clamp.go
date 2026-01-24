package utils

import "cmp"

func Clamp[T cmp.Ordered](value, minValue, maxValue T) T {
	return min(max(value, minValue), maxValue)
}
