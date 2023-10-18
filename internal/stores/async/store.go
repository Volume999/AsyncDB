package async

import "POCS_Projects/internal/benchmark/databases"

type Store[V any, K any] interface {
	Put(ctx *databases.ConnectionContext, value V) <-chan databases.RequestResult
	Get(ctx *databases.ConnectionContext, key K) <-chan databases.RequestResult
	Delete(ctx *databases.ConnectionContext, key K) <-chan databases.RequestResult
}
