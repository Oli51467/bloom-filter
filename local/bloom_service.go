package local

import "github.com/demdxx/gocast"

// BloomService
// m：bimap 的长度，由用户输入
// k：hash 函数的个数，由用户输入
// n：布隆过滤器中的元素个数，由布隆过滤器统计
// bitmap：位图，类型为 []int，其中使用到每个 int 元素的 32 个 bit 位，因此有 []int 长度为 m/32.
// encryptor：散列函数编码模块
type BloomService struct {
	m, k, n   int32
	bitmap    []int
	encryptor *Encryptor
}

func NewBloomService(m, k int32, encryptor *Encryptor) *BloomService {
	return &BloomService{
		m:         m,
		k:         k,
		bitmap:    make([]int, m/32+1), // 为避免除不尽的问题，切片长度额外增大 1
		encryptor: encryptor,
	}
}

// Exist 判定一个元素 val 是否存在于布隆过滤器
func (l *BloomService) Exist(val string) bool {
	// 基于 BloomService.getKEncrypted 方法，获取到 val 对应的 k 个 bit 位的偏移 offset
	// []int中每个 int 元素使用 32 个 bit 位，因此对于每个 offset，对应在 []int 中的 index 位置为 offset >> 5，即 offset/32
	for _, idx := range l.getKEncrypted(val) {
		index := idx >> 5  // 等价于 / 32 指示该bit应该放到切片的哪个下标中
		offset := idx & 31 // 等价于 % 32

		// 其中的某一位为0 一定不存在
		if l.bitmap[index]&(1<<offset) == 0 {
			return false
		}
	}

	return true
}

// getKEncrypted 获取一个元素 val 对应 k 个 bit 位偏移量 offset
func (l *BloomService) getKEncrypted(val string) []int32 {
	encrypts := make([]int32, 0, l.k)
	origin := val
	// 首次映射时，以元素 val 作为输入，获取 murmur3 映射得到的 hash 值
	// 接下来每次以上一轮的 hash 值作为输入，获取 murmur3 映射得到新一轮 hash 值
	// 凑齐 k 个 hash 值后返回结果
	for i := 0; int32(i) < l.k; i++ {
		encrypted := l.encryptor.Encrypt(origin)
		encrypts = append(encrypts, encrypted%l.m)
		if int32(i) == l.k-1 {
			break
		}
		origin = gocast.ToString(encrypted)
	}
	return encrypts
}

// Set 追加元素进入布隆过滤器
func (l *BloomService) Set(val string) {
	// 每有一个新元素到来，布隆过滤器中的 n 递增
	l.n++
	// 调用 LocalBloomService.getKEncrypted 方法，获取到元素 val 对应的 k 个 bit 位的偏移量 offset
	for _, offset := range l.getKEncrypted(val) {
		// 通过 offset >> 5 获取到 bit 位在 []int 中的索引
		index := offset >> 5 // 等价于 / 32
		// 通过 offset & 31 获取到 bit 位在 int 中的 bit 位置
		bitOffset := offset & 31 // 等价于 % 32

		// 通过 | 操作，将对应的 bit 位置为 1
		l.bitmap[index] |= 1 << bitOffset
	}
}
