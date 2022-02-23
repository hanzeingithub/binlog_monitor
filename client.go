package binlog_monitor

import (
	"os"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/hanzeingithub/hlog"
)

type Client struct {
	config  *monitorConfig
	Handler *monitorHandler
	runner  *canal.Canal
}

func GetDefaultClient() *Client {
	c := &Client{}
	c.config = getDefaultConf()
	c.Handler = getDefaultHandler(c.config)
	return c
}

func GetClientWithConfig(config *canal.Config) *Client {
	c := &Client{}
	c.config.canalConf = config
	c.Handler = &monitorHandler{}
	return c
}

func (c *Client) Start() {
	c.initCanal()
	pos, err := c.runner.GetMasterPos()
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]get master pos failed: %+v", err)
		os.Exit(1)
	}
	if err := c.runner.RunFrom(pos); err != nil {
		hlog.ErrorOf("[binlog_monitor]run monitor failed: %+v", err)
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
	c.runner, err = canal.NewCanal(c.config.canalConf)
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]get canal failed, err:%+v", err)
		os.Exit(-1)
	}
	c.runner.SetEventHandler(c.Handler)
}

func (c *Client) Register(tableName, topic string, action ActionType) {
	if _, ok := c.Handler.topicMap[tableName]; !ok {
		c.Handler.topicMap[tableName] = make(map[ActionType]string)
	}
	c.Handler.topicMap[tableName][action] = topic
}

func (c *Client) Stop() {
	c.runner.Close()
}

func (c *Client) SetOnXIDHandler(funcHandler OnXIDHandler) {
	c.Handler.OnXIDHandler = funcHandler
}

func (c *Client) SetOnGTIDHandler(funcHandler OnGTIDHandler) {
	c.Handler.OnGTIDHandler = funcHandler
}

func (c *Client) SetOnRowHandler(funcHandler OnRowHandler) {
	c.Handler.OnRowHandler = funcHandler
}

func (c *Client) SetOnRotateHandler(funcHandler OnRotateHandler) {
	c.Handler.OnRotateHandler = funcHandler
}

func (c *Client) SetOnPosSyncedHandler(funcHandler OnPosSyncedHandler) {
	c.Handler.OnPosSyncedHandler = funcHandler
}

func (c *Client) SetOnTableChangedHandler(funcHandler OnTableChangedHandler) {
	c.Handler.OnTableChangedHandler = funcHandler
}

func (c *Client) SetOnDDLHandler(funcHandler OnDDLHandler) {
	c.Handler.OnDDLHandler = funcHandler
}
