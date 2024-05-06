package rpctry

import (
	"context"
	"crypto/tls"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/gwaylib/errors"
)

type Client interface {
	Close() error

	Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error

	// Call rpc server timeout with 30s
	TryCall(serviceMethod string, args interface{}, reply interface{}) error
}

type rpcClient struct {
	addr      string
	tlsCfg    *tls.Config
	mux       sync.Mutex
	connected bool
	client    *rpc.Client
}

func NewClient(addr string) Client {
	return &rpcClient{
		addr: addr,
	}
}

func NewTlsClient(addr string, config *tls.Config) Client {
	return &rpcClient{
		addr:   addr,
		tlsCfg: config,
	}
}

func (rc *rpcClient) conn() (*rpc.Client, error) {
	rc.mux.Lock()
	defer rc.mux.Unlock()

	if rc.connected {
		return rc.client, nil
	}

	conn, err := net.DialTimeout("tcp", rc.addr, 10*time.Second)
	if err != nil {
		return nil, errors.As(err, rc.addr)
	}
	if rc.tlsCfg != nil {
		rc.client = rpc.NewClient(tls.Client(conn, rc.tlsCfg))
	} else {
		rc.client = rpc.NewClient(conn)
	}
	rc.connected = true

	return rc.client, nil
}

func (rc *rpcClient) disconn() error {
	rc.mux.Lock()
	defer rc.mux.Unlock()

	if rc.connected {
		rc.connected = false
		return rc.client.Close()
	}
	return nil
}

func (rc *rpcClient) call(ctx context.Context, method string, args interface{}, reply interface{}) error {
	retried := false
_retry:
	client, err := rc.conn()
	if err != nil {
		return err
	}

	select {
	case call := <-client.Go(method, args, reply, make(chan *rpc.Call, 1)).Done:
		err := call.Error
		if errors.Equal(rpc.ErrShutdown, err) {
			rc.disconn()
			if !retried {
				retried = true
				goto _retry
			}
		}
		return errors.As(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rc *rpcClient) Close() error {
	return rc.disconn()
}

func (rc *rpcClient) Call(ctx context.Context, method string, args interface{}, reply interface{}) error {
	return rc.call(ctx, method, args, reply)
}

func (rc *rpcClient) TryCall(method string, args interface{}, reply interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return rc.call(ctx, method, args, reply)
}
