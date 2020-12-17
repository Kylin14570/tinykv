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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	} //调用CreateDB函数创建一个数据库
	//对StandAloneStorage进行实例化并返回
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

//Reader函数要返回一个StorageReader接口
//需要先在上面定义StandAloneStorageReader结构体，对其进行实例化
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
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

	for _, tmp := range batch {

		switch tmp.Data.(type) { //选择操作类型

		case storage.Put: //写
			err := engine_util.PutCF(s.db, tmp.Cf(), tmp.Key(), tmp.Value())
			if err != nil {
				return err //出错立即返回
			}

		case storage.Delete: //读
			err := engine_util.DeleteCF(s.db, tmp.Cf(), tmp.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
