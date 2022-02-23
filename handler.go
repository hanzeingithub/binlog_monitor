package binlog_monitor

import (
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/hanzeingithub/hlog"
)

type monitorHandler struct {
	canal.DummyEventHandler
	producer     sarama.SyncProducer
	topicMap     map[string]map[ActionType]string
	defaultTopic string
	OnXIDHandler
	OnGTIDHandler
	OnRowHandler
	OnRotateHandler
	OnPosSyncedHandler
	OnTableChangedHandler
	OnDDLHandler
}

type OnXIDHandler func(nextPos mysql.Position) error
type OnGTIDHandler func(gtid mysql.GTIDSet) error
type OnRowHandler func(ev *canal.RowsEvent) error
type OnTableChangedHandler func(schema string, table string) error
type OnPosSyncedHandler func(pos mysql.Position, set mysql.GTIDSet, force bool) error
type OnRotateHandler func(r *replication.RotateEvent) error
type OnDDLHandler func(nextPos mysql.Position, queryEvent *replication.QueryEvent) error

func getDefaultHandler(conf *monitorConfig) *monitorHandler {
	handler := &monitorHandler{
		topicMap: make(map[string]map[ActionType]string),
	}
	kafkaClient, err := sarama.NewClient(conf.clientAddr, conf.kafkaConf)
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]get kafka client failed:%v", err)
		os.Exit(-1)
	}
	handler.producer, err = sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]create kafka producer failed:%v", err)
		os.Exit(-1)
	}
	hlog.InfoOf("[binlog_monitor]create kafka producer success!")
	initHandler(handler)
	handler.defaultTopic = "binlog_monitor"
	return handler
}

func initHandler(handler *monitorHandler) {
	handler.OnXIDHandler = func(nextPos mysql.Position) error {
		return nil
	}

	handler.OnGTIDHandler = func(gtid mysql.GTIDSet) error {
		return nil
	}

	handler.OnRowHandler = func(ev *canal.RowsEvent) error {
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
		msg := &sarama.ProducerMessage{
			Topic: handler.getTopic(message.TableName, message.Action),
			Value: sarama.ByteEncoder(jsonString),
		}
		return handler.sendMessage(msg)
	}

	handler.OnTableChangedHandler = func(schema string, table string) error {
		return nil
	}

	handler.OnPosSyncedHandler = func(pos mysql.Position, set mysql.GTIDSet, force bool) error {
		return nil
	}

	handler.OnRotateHandler = func(r *replication.RotateEvent) error {
		return nil
	}

	handler.OnDDLHandler = func(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
		return nil
	}
}

type BinLogMessage struct {
	TableName string
	DBName    string
	Action    ActionType
	Data      [][]interface{}
}

func (h *monitorHandler) OnXID(nextPos mysql.Position) error {
	return h.OnXIDHandler(nextPos)
}

func (h *monitorHandler) OnGTID(gtid mysql.GTIDSet) error {
	return h.OnGTIDHandler(gtid)
}

func (h *monitorHandler) OnRow(ev *canal.RowsEvent) error {
	return h.OnRowHandler(ev)
}

func (h *monitorHandler) OnTableChanged(schema string, table string) error {
	return h.OnTableChangedHandler(schema, table)
}

func (h *monitorHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return h.OnPosSyncedHandler(pos, set, force)
}

func (h *monitorHandler) OnRotate(r *replication.RotateEvent) error {
	return h.OnRotateHandler(r)
}

func (h *monitorHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return h.OnDDLHandler(nextPos, queryEvent)
}

func (h *monitorHandler) String() string {
	return "monitorHandler"
}

func (h *monitorHandler) getTopic(tableName string, actionType ActionType) string {
	if tableMap, ok := h.topicMap[tableName]; ok {
		if topic, ok := tableMap[Any]; ok {
			return topic
		}
		if topic, ok := tableMap[actionType]; ok {
			return topic
		}
	}
	return h.defaultTopic
}

func (h *monitorHandler) sendMessage(msg *sarama.ProducerMessage) error {
	_, _, err := h.producer.SendMessage(msg)
	if err != nil {
		hlog.ErrorOf("[binlog_monitor]send msg failed, err:%+v", err)
		return err
	}
	//hlog.InfoOf("[binlog_monitor]send msg success, pid:%v offset:%v\n", pid, offset)
	return nil
}
