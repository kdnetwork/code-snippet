//go:build !cgo

package db

import "github.com/glebarez/sqlite"

var SqliteDriverOpen = sqlite.Open

const CgoEnabled = false
