package binlog_monitor

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

type ActionType int

const (
	Unknown ActionType = iota
	Delete  ActionType = 1
	Insert  ActionType = 2
	Update  ActionType = 3
	Any     ActionType = 4
)

func GetAction(str string) ActionType {
	switch str {
	case canal.DeleteAction:
		return Delete
	case canal.InsertAction:
		return Insert
	case canal.UpdateAction:
		return Update
	default:
		return Unknown
	}
}
