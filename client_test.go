package binlog_monitor

import (
	"testing"
)

func TestSome(t *testing.T) {
	cli := GetDefaultClient()
	cli.Start()
}
