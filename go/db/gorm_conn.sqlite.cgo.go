//go:build cgo

package db

import "gorm.io/driver/sqlite"

var SqliteDriverOpen = sqlite.Open

const CgoEnabled = true
