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
	// Your Data Here (1).
	db   *badger.DB
	opts *badger.Options
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	s := &StandAloneStorage{
		db:   nil,
		opts: &opts,
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db, err := badger.Open(*s.opts)
	if err != nil {
		return nil
	}

	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

type PrivateReader struct {
	// GetCF(cf string, key []byte) ([]byte, error)
	// IterCF(cf string) engine_util.DBIterator
	// Close()
	txn *badger.Txn
}

func (r PrivateReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(r.txn, cf, key)
	return val, nil
}

func (r PrivateReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r PrivateReader) Close() {
	r.txn.Commit()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	r := PrivateReader{
		txn: *&txn,
	}
	return r, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Delete:
			err := txn.Delete(engine_util.KeyWithCF(v.Cf(), v.Key()))
			if err != nil {
				// FIXME:
				return nil
			}
		case storage.Put:
			err := txn.Set(engine_util.KeyWithCF(v.Cf(), v.Key()), v.Value())
			if err != nil {
				// FIXME:
				return nil
			}
		}
	}
	_ = txn.Commit()

	return nil
}
