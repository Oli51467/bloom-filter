package redis

import (
	"context"
	"github.com/gomodule/redigo/redis"
)

// Client 基于 redis 连接池，进行连接的复用，每次操作需要先从连接池获取连接，使用完毕后需要手动将连接放回池子中
type Client struct {
	pool *redis.Pool
}

func NewClient(pool *redis.Pool) *Client {
	return &Client{
		pool: pool,
	}
}

// Eval 执行 lua 脚本，保证复合操作的原子性
func (r *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)

	// 获取连接
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}

	// 放回连接池
	defer func(conn redis.Conn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	// 执行 lua 脚本
	return conn.Do("EVAL", args...)
}
