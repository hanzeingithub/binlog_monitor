package binlog_monitor

import (
	"os"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/hanzeingithub/hlog"
)

type Client struct {
	config  *canal.Config
	handler canal.EventHandler
	runner  *canal.Canal
}

func GetDefaultClient() *Client {
	c := &Client{}
	c.config = canal.NewDefaultConfig()
	c.handler = &defaultHandler{}
	return c
}

func GetClientWithConfig(config *canal.Config) *Client {
	c := &Client{}
	c.config = config
	c.handler = &defaultHandler{}
	return c
}

func (c *Client) SetHandler(handler canal.EventHandler) {
	c.handler = handler
}

func (c *Client) Start() {
	c.initCanal()
	pos, err := c.runner.GetMasterPos()
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]get master pos failed: %+v", err)
		os.Exit(1)
	}
	if err := c.runner.RunFrom(pos); err != nil {
		hlog.ErrorOf("[binlog_monitor]start monitor failed: %+v", err)
		os.Exit(1)
	}
}

func (c *Client) StartWithPos(pos *mysql.Position) {
	c.initCanal()
	if err := c.runner.RunFrom(*pos); err != nil {
		hlog.ErrorOf("[binlog_monitor]start monitor failed: %+v", err)
		os.Exit(1)
	}
}

func (c *Client) initCanal() {
	var err error
	c.runner, err = canal.NewCanal(c.config)
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]get canal failed, err:%+v", err)
		os.Exit(-1)
	}
	c.runner.SetEventHandler(c.handler)
}

func (c *Client) Stop() {
	c.runner.Close()
}
