package binlog_monitor

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
)

func TestSome(t *testing.T) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = ""
	cfg.Dump.TableDB = "open_core"
	client := GetClientWithConfig(cfg)
	client.Start()
}
