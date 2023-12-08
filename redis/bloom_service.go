package redis

import (
	"context"
	"fmt"
	"github.com/demdxx/gocast"
)

// LuaBloomBatchSetBits Lua脚本 将 k 个 bit 位同时置为 1
const LuaBloomBatchSetBits = `
  local bloomKey = KEYS[1]
  local bitsCnt = ARGV[1]

  for i=1,bitsCnt,1 do
    local offset = ARGV[1+i]
    redis.call('setbit',bloomKey,offset,1)
  end
  return 1
`

// LuaBloomBatchGetBits 批量执行 bitmap 查询操作的 lua 脚本：会针对 k 个 bit 位进行查询，只要有一个 bit 位的标识为 0，则返回 0；如果所有 bit 位的标识都为 1，则返回 1.
const LuaBloomBatchGetBits = `
  local bloomKey = KEYS[1]
  local bitsCnt = ARGV[1]
  for i=1,bitsCnt,1 do
    local offset = ARGV[1+i]
    local reply = redis.call('getbit',bloomKey,offset)
    if (not reply) then
        error('FAIL')
        return 0
    end
    if (reply == 0) then
        return 0
    end
  end
  return 1
`

// BloomService 布隆过滤器服务
type BloomService struct {
	m, k      int32
	encryptor *Encryptor
	client    *Client
}

// NewBloomService
// m -> bitmap 的长度；k -> hash 函数的个数；
// client -> redis 客户端；encryptor -> hash 映射器
func NewBloomService(m, k int32, client *Client, encryptor *Encryptor) *BloomService {
	return &BloomService{
		m:         m,
		k:         k,
		client:    client,
		encryptor: encryptor,
	}
}

// Exist key: 布隆过滤器 bitmap 对应的 key  val: 基于 hash 映射到 bitmap 中的值
func (b *BloomService) Exist(ctx context.Context, key, val string) (bool, error) {
	// 映射对应的 bit 位
	keyAndArgs := make([]interface{}, 0, b.k+2)
	keyAndArgs = append(keyAndArgs, key, b.k)
	for _, encrypted := range b.getKEncrypted(val) {
		keyAndArgs = append(keyAndArgs, encrypted)
	}

	rawResp, err := b.client.Eval(ctx, LuaBloomBatchGetBits, 1, keyAndArgs)
	if err != nil {
		return false, err
	}

	resp := gocast.ToInt(rawResp)
	if resp == 1 {
		return true, nil
	}
	return false, nil
}

// Set 将一个输入元素添加到布隆过滤器中
// key 对应的是布隆过滤器中 bitmap 的标识键 key，不同 key 对应的元素是相互隔离的
// val 对应的是输入的元素，从属于某个 key 对应的 bitmap
func (b *BloomService) Set(ctx context.Context, key, val string) error {
	// 映射对应的 bit 位
	keyAndArgs := make([]interface{}, 0, b.k+2)
	keyAndArgs = append(keyAndArgs, key, b.k)
	// 调用 BloomService.getKEncrypted 方法，获取到 k 个 bit 位对应的偏移量 offset
	for _, encrypted := range b.getKEncrypted(val) {
		keyAndArgs = append(keyAndArgs, encrypted)
	}

	// 调用 RedisClient.Eval 方法执行 lua 脚本，将 k 个 bit 位统统置为 1
	rawResp, err := b.client.Eval(ctx, LuaBloomBatchSetBits, 1, keyAndArgs)
	if err != nil {
		return err
	}

	resp := gocast.ToInt(rawResp)
	if resp != 1 {
		return fmt.Errorf("resp: %d", resp)
	}
	return nil
}

// getKEncrypted 获取一个元素 val 对应 k 个 bit 位偏移量 offset
func (b *BloomService) getKEncrypted(val string) []int32 {
	encrypts := make([]int32, 0, b.k)
	origin := val
	for i := 0; int32(i) < b.k; i++ {
		encrypted := b.encryptor.Encrypt(origin)
		encrypts = append(encrypts, encrypted)
		if int32(i) == b.k-1 {
			break
		}
		origin = gocast.ToString(encrypted)
	}
	return encrypts
}
