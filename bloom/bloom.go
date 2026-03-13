package bloom

import (
	"hash/fnv"
	"math"
	"sync"
)

type BloomFilter struct {
	mu   sync.RWMutex
	bits []uint64
	m    uint // 位数组长度
	k    uint // 哈希函数个数
}

// NewBloomFilter n: 预期存储的元素量, p: 允许的误判率 (如 0.01)
func NewBloomFilter(n uint, p float64) *BloomFilter {
	m := uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k := uint(math.Ceil(float64(m) / float64(n) * math.Log(2)))
	return &BloomFilter{
		bits: make([]uint64, (m+63)/64),
		m:    m,
		k:    k,
	}
}

func (bf *BloomFilter) Add(key string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := uint(0); i < bf.k; i++ {
		offset := bf.hash(key, i) % bf.m
		bf.bits[offset/64] |= (1 << (offset % 64))
	}
}

func (bf *BloomFilter) Contains(key string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for i := uint(0); i < bf.k; i++ {
		offset := bf.hash(key, i) % bf.m
		if bf.bits[offset/64]&(1<<(offset%64)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(key string, seed uint) uint {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{byte(seed)})
	return uint(h.Sum64())
}
