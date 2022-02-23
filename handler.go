package binlog_monitor

import (
	"encoding/json"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/hanzeingithub/hlog"
)

type defaultHandler struct {
	canal.DummyEventHandler
}

type BinLogMessage struct {
	TableName string
	DBName    string
	Action    ActionType
	Data      [][]interface{}
}

//监听数据记录
func (h *defaultHandler) OnRow(ev *canal.RowsEvent) error {
	message := &BinLogMessage{
		TableName: ev.Table.Name,
		DBName:    ev.Table.Schema,
		Action:    GetAction(ev.Action),
		Data:      ev.Rows,
	}
	jsonString, err := json.Marshal(message)
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]json marshal failed, error:%+v", err)
		return err
	}
	hlog.InfoOf("[binlog_monitor]OnRow called, send message:%d", jsonString)

	return nil
}

//创建、更改、重命名或删除表时触发，通常会需要清除与表相关的数据，如缓存。It will be called before OnDDL.
func (h *defaultHandler) OnTableChanged(schema string, table string) error {
	return nil
}

//监听binlog日志的变化文件与记录的位置
func (h *defaultHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

//当产生新的binlog日志后触发(在达到内存的使用限制后（默认为 1GB），会开启另一个文件，每个新文件的名称后都会有一个增量。)
func (h *defaultHandler) OnRotate(r *replication.RotateEvent) error {
	return nil

}

// create alter drop truncate(删除当前表再新建一个一模一样的表结构)
func (h *defaultHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (h *defaultHandler) String() string {
	return "defaultHandler"
}
