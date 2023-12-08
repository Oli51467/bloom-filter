package redis

import (
	"github.com/spaolacci/murmur3"
	"math"
)

type Encryptor struct {
}

func NewEncryptor() *Encryptor {
	return &Encryptor{}
}

// Encrypt 通过 murmur3 实现的 hash 编码模块，将输入的字符串转为 int32 类型的 hash 值
func (e *Encryptor) Encrypt(origin string) int32 {
	hash := murmur3.New32()
	_, _ = hash.Write([]byte(origin))
	return int32(hash.Sum32() % math.MaxInt32)
}
