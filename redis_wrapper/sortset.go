package redis_wrapper

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

func (w *redisWrapper) SortSetAdd(key string, items ...SortSetItem) error {
	if len(items) == 0 {
		return nil
	}

	var args = redis.Args([]interface{}{}).Add(key)
	for _, item := range items {
		switch item.GetScore().(type) {
		case int, int16, int32, int64, int8:
		case uint8, uint16, uint32, uint64:
		case float32, float64:
		default:
			return fmt.Errorf("item %s 's socre(%v) not allowd ", item.GetMember(), item.GetScore())
		}
		args = args.Add(item.GetScore(), item.GetMember())
	}

	var c = w.pool.Get()
	var _, err = c.Do("ZADD", args...)
	_ = c.Close()
	return err
}

func (w *redisWrapper) SortSetLen(key string) (int, error) {
	var c = w.pool.Get()
	var n, err = redis.Int(c.Do("ZCARD", key))
	_ = c.Close()
	return n, err
}

func (w *redisWrapper) SortSetRemove(key string, names ...string) error {
	if len(names) == 0 {
		return nil
	}

	var args = redis.Args([]interface{}{}).Add(key)
	args = args.AddFlat(names)

	var c = w.pool.Get()
	var _, err = c.Do("ZREM", args...)
	_ = c.Close()
	return err
}
