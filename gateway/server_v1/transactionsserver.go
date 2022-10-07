package server_v1

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	transactions_v1 "github.com/couchbase/stellar-nebula/genproto/transactions/v1"
)

type TransactionsServer struct {
	transactions_v1.UnimplementedTransactionsServer

	cbClient *gocb.Cluster
	txnMgr   *gocbcore.TransactionsManager
	txns     map[string]*gocbcore.Transaction
	txnsLock sync.Mutex
}

func (s *TransactionsServer) getAgent(bucketName string) (*gocbcore.Agent, error) {
	return s.cbClient.Bucket(bucketName).Internal().IORouter()
}

func (s *TransactionsServer) getTransaction(bucketName string, txnId string) (*gocbcore.Transaction, error) {
	s.txnsLock.Lock()
	txn, ok := s.txns[txnId]
	s.txnsLock.Unlock()

	if !ok {
		return nil, errors.New("invalid transaction id")
	}

	return txn, nil
}

func (s *TransactionsServer) getAttempt(bucketName string, txnId string, attemptId string) (*gocbcore.Transaction, error) {
	txn, err := s.getTransaction(bucketName, txnId)
	if err != nil {
		return nil, err
	}

	if txn.Attempt().ID != attemptId {
		return nil, errors.New("invalid attempt id")
	}

	return txn, nil
}

func (s *TransactionsServer) asyncOp(fn func(cb func()) error) error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	err := fn(func() {
		wg.Done()
	})
	if err != nil {
		return err
	}

	wg.Wait()
	return nil
}

func (s *TransactionsServer) TransactionBeginAttempt(
	ctx context.Context,
	in *transactions_v1.TransactionBeginAttemptRequest,
) (*transactions_v1.TransactionBeginAttemptResponse, error) {
	if in.TransactionId != nil {
		txnId := *in.TransactionId
		txn, err := s.getTransaction(in.BucketName, txnId)
		if err != nil {
			return nil, err
		}

		err = txn.NewAttempt()
		if err != nil {
			return nil, err
		}

		return &transactions_v1.TransactionBeginAttemptResponse{
			TransactionId: txnId,
			AttemptId:     txn.Attempt().ID,
		}, nil
	}

	txn, err := s.txnMgr.BeginTransaction(&gocbcore.TransactionOptions{
		DurabilityLevel: gocbcore.TransactionDurabilityLevelMajority,
	})
	if err != nil {
		return nil, err
	}

	err = txn.NewAttempt()
	if err != nil {
		return nil, err
	}

	txnId := txn.ID()
	// TODO(brett19): Need to cleanup these txns after they are expired.
	s.txnsLock.Lock()
	s.txns[txnId] = txn
	s.txnsLock.Unlock()

	return &transactions_v1.TransactionBeginAttemptResponse{
		TransactionId: txnId,
		AttemptId:     txn.Attempt().ID,
	}, nil
}

func (s *TransactionsServer) TransactionCommit(
	ctx context.Context,
	in *transactions_v1.TransactionCommitRequest,
) (respOut *transactions_v1.TransactionCommitResponse, errOut error) {
	txn, err := s.getAttempt(in.BucketName, in.TransactionId, in.AttemptId)
	if err != nil {
		return nil, err
	}

	err = s.asyncOp(func(cb func()) error {
		return txn.Commit(func(err error) {
			if err != nil {
				errOut = err
				cb()
				return
			}

			respOut = &transactions_v1.TransactionCommitResponse{}
			errOut = err
			cb()
		})
	})
	if err != nil {
		return nil, err
	}

	return
}

func (s *TransactionsServer) TransactionRollback(
	ctx context.Context,
	in *transactions_v1.TransactionRollbackRequest,
) (respOut *transactions_v1.TransactionRollbackResponse, errOut error) {
	txn, err := s.getAttempt(in.BucketName, in.TransactionId, in.AttemptId)
	if err != nil {
		return nil, err
	}

	err = s.asyncOp(func(cb func()) error {
		return txn.Rollback(func(err error) {
			if err != nil {
				errOut = err
				cb()
				return
			}

			respOut = &transactions_v1.TransactionRollbackResponse{}
			errOut = err
			cb()
		})
	})
	if err != nil {
		return nil, err
	}

	return
}

func (s *TransactionsServer) TransactionGet(
	ctx context.Context,
	in *transactions_v1.TransactionGetRequest,
) (respOut *transactions_v1.TransactionGetResponse, errOut error) {
	txn, err := s.getAttempt(in.BucketName, in.TransactionId, in.AttemptId)
	if err != nil {
		return nil, err
	}

	agent, err := s.getAgent(in.BucketName)
	if err != nil {
		return nil, err
	}

	err = s.asyncOp(func(cb func()) error {
		return txn.Get(gocbcore.TransactionGetOptions{
			Agent:          agent,
			ScopeName:      in.ScopeName,
			CollectionName: in.CollectionName,
			Key:            []byte(in.Key),
			OboUser:        "",
		}, func(res *gocbcore.TransactionGetResult, err error) {
			if err != nil {
				errOut = err
				cb()
				return
			}

			respOut = &transactions_v1.TransactionGetResponse{
				Cas:   uint64(res.Cas),
				Value: res.Value,
			}
			errOut = err
			cb()
		})
	})
	if err != nil {
		return nil, err
	}

	return
}

func (s *TransactionsServer) TransactionInsert(
	ctx context.Context,
	in *transactions_v1.TransactionInsertRequest,
) (respOut *transactions_v1.TransactionInsertResponse, errOut error) {
	txn, err := s.getAttempt(in.BucketName, in.TransactionId, in.AttemptId)
	if err != nil {
		return nil, err
	}

	agent, err := s.getAgent(in.BucketName)
	if err != nil {
		return nil, err
	}

	err = s.asyncOp(func(cb func()) error {
		return txn.Insert(gocbcore.TransactionInsertOptions{
			Agent:          agent,
			ScopeName:      in.ScopeName,
			CollectionName: in.CollectionName,
			Key:            []byte(in.Key),
			Value:          in.Value,
			OboUser:        "",
		}, func(res *gocbcore.TransactionGetResult, err error) {
			if err != nil {
				errOut = err
				cb()
				return
			}

			respOut = &transactions_v1.TransactionInsertResponse{
				Cas: uint64(res.Cas),
			}
			errOut = err
			cb()
		})
	})
	if err != nil {
		return nil, err
	}

	return
}

func (s *TransactionsServer) TransactionReplace(
	ctx context.Context,
	in *transactions_v1.TransactionReplaceRequest,
) (respOut *transactions_v1.TransactionReplaceResponse, errOut error) {
	txn, err := s.getAttempt(in.BucketName, in.TransactionId, in.AttemptId)
	if err != nil {
		return nil, err
	}

	agent, err := s.getAgent(in.BucketName)
	if err != nil {
		return nil, err
	}

	txnDoc := s.txnMgr.Internal().CreateGetResult(gocbcore.TransactionCreateGetResultOptions{
		Agent:          agent,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
		Key:            []byte(in.Key),
		Cas:            gocbcore.Cas(in.Cas),
		Meta:           nil,
		OboUser:        "",
	})

	err = s.asyncOp(func(cb func()) error {
		return txn.Replace(gocbcore.TransactionReplaceOptions{
			Document: txnDoc,
			Value:    in.Value,
		}, func(res *gocbcore.TransactionGetResult, err error) {
			if err != nil {
				errOut = err
				cb()
				return
			}

			respOut = &transactions_v1.TransactionReplaceResponse{
				Cas: uint64(res.Cas),
			}
			errOut = err
			cb()
		})
	})
	if err != nil {
		return nil, err
	}

	return
}

func (s *TransactionsServer) TransactionRemove(
	ctx context.Context,
	in *transactions_v1.TransactionRemoveRequest,
) (respOut *transactions_v1.TransactionRemoveResponse, errOut error) {
	txn, err := s.getAttempt(in.BucketName, in.TransactionId, in.AttemptId)
	if err != nil {
		return nil, err
	}

	agent, err := s.getAgent(in.BucketName)
	if err != nil {
		return nil, err
	}

	txnDoc := s.txnMgr.Internal().CreateGetResult(gocbcore.TransactionCreateGetResultOptions{
		Agent:          agent,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
		Key:            []byte(in.Key),
		Cas:            gocbcore.Cas(in.Cas),
		Meta:           nil,
		OboUser:        "",
	})

	err = s.asyncOp(func(cb func()) error {
		return txn.Remove(gocbcore.TransactionRemoveOptions{
			Document: txnDoc,
		}, func(res *gocbcore.TransactionGetResult, err error) {
			if err != nil {
				errOut = err
				cb()
				return
			}

			respOut = &transactions_v1.TransactionRemoveResponse{
				Cas: uint64(res.Cas),
			}
			errOut = err
			cb()
		})
	})
	if err != nil {
		return nil, err
	}

	return
}

func NewTransactionsServer(cbClient *gocb.Cluster) *TransactionsServer {
	txnMgr, err := gocbcore.InitTransactions(&gocbcore.TransactionsConfig{})
	if err != nil {
		log.Printf("failed to initialize transactions manager: %s", err)
	}

	return &TransactionsServer{
		cbClient: cbClient,
		txnMgr:   txnMgr,
		txns:     make(map[string]*gocbcore.Transaction),
	}
}
