package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, nil
	}
	v, _ := reader.GetCF(req.Cf, req.Key)
	resp := &kvrpcpb.RawGetResponse{}
	if v == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = v
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = "error"
		return resp, nil
	}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawScanResponse{}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)

	var i uint32
	for i = 0; i < req.Limit; i++ {
		if !iter.Valid() {
			break;
		}
		v, _ := iter.Item().Value()
		kv := &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: v,
		}
		resp.Kvs = append(resp.Kvs, kv)
		iter.Next()
	}
	iter.Close()
	reader.Close()
	return resp, nil
}
