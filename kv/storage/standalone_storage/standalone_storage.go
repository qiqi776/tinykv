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
	cfg *config.Config
	db  *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		cfg: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	s.db = engine_util.CreateDB(s.cfg.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

type standAloneReader struct {
	txn *badger.Txn
}

func (r *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *standAloneReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &standAloneReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// 创建事务
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	// 循环读取batch
	for _, modify := range batch {
		switch data := modify.Data.(type) {
		case storage.Put:
			err := txn.Set(
				engine_util.KeyWithCF(data.Cf, data.Key),
				data.Value,
			)
			if err != nil {
				return err
			}
		case storage.Delete:
			err := txn.Delete(
				engine_util.KeyWithCF(data.Cf, data.Key),
			)
			if err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}
