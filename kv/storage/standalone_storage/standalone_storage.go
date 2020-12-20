package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	confg *config.Config //配置
	db    *badger.DB     //数据库
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		confg: conf, //保留配置
	}
}

func (s *StandAloneStorage) Start() error {
	//初始化：根据配置创建数据库
	s.db = engine_util.CreateDB(s.confg.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	//关闭数据库
	return s.db.Close()
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

//Reader函数要返回一个StorageReader接口
//需要先在上面定义StandAloneStorageReader结构体，对其进行实例化
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{
		txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil { //出错，说明没找到
		return nil, nil
	}
	return value, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	for _, tmp := range batch { //遍历所有修改操作

		switch tmp.Data.(type) { //选择操作类型

		case storage.Put: //写
			err := engine_util.PutCF(s.db, tmp.Cf(), tmp.Key(), tmp.Value())
			if err != nil {
				return err
			}

		case storage.Delete: //删除
			err := engine_util.DeleteCF(s.db, tmp.Cf(), tmp.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
