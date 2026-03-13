package kamacache

import (
	"context"
	"github.com/youngyangyang04/KamaCache-Go/bloom"
	"github.com/youngyangyang04/KamaCache-Go/singleflight"
	"github.com/youngyangyang04/KamaCache-Go/store"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// Cache 是对底层缓存存储的封装
type Cache struct {
	mu sync.RWMutex

	bf     *bloom.BloomFilter  // 布隆过滤器
	sfg    *singleflight.Group // 引用你 singleflight.go 中的 Group
	loader Getter              // 回源接口

	store       store.Store  // 底层存储实现
	opts        CacheOptions // 缓存配置选项
	hits        int64        // 缓存命中次数
	misses      int64        // 缓存未命中次数
	initialized int32        // 原子变量，标记缓存是否已初始化
	closed      int32        // 原子变量，标记缓存是否已关闭
}

// CacheOptions 缓存配置选项
type CacheOptions struct {
	CacheType         store.CacheType                     // 缓存类型: LRU, LRU2 等
	MaxBytes          int64                               // 最大内存使用量
	BucketCount       uint16                              // 缓存桶数量 (用于 LRU2)
	CapPerBucket      uint16                              // 每个缓存桶的容量 (用于 LRU2)
	Level2Cap         uint16                              // 二级缓存桶的容量 (用于 LRU2)
	CleanupTime       time.Duration                       // 清理间隔
	OnEvicted         func(key string, value store.Value) // 驱逐回调
	DefaultTTL        time.Duration                       //默认生存时间
	ExpectedElements  uint                                //n: 预期存储的元素量,
	FalsePositiveRate float64                             //p: 允许的误判率
}

// DefaultCacheOptions 返回默认的缓存配置
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:         store.LRU2,
		MaxBytes:          8 * 1024 * 1024, // 8MB
		BucketCount:       16,
		CapPerBucket:      512,
		Level2Cap:         256,
		CleanupTime:       time.Minute,
		OnEvicted:         nil,
		DefaultTTL:        time.Minute,
		ExpectedElements:  1000000, //n: 预期存储的元素量,
		FalsePositiveRate: 0.01,    //p: 允许的误判率
	}
}

// NewCache 创建一个新的缓存实例
func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
		bf:   bloom.NewBloomFilter(opts.ExpectedElements, opts.FalsePositiveRate),
		sfg:  &singleflight.Group{},
	}
}

// ensureInitialized 确保缓存已初始化
func (c *Cache) ensureInitialized() {
	// 快速检查缓存是否已初始化，避免不必要锁争用
	if atomic.LoadInt32(&c.initialized) == 1 {
		return
	}

	// 双重检查锁定模式
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialized == 0 {
		// 创建存储选项
		storeOpts := store.Options{
			MaxBytes:        c.opts.MaxBytes,
			BucketCount:     c.opts.BucketCount,
			CapPerBucket:    c.opts.CapPerBucket,
			Level2Cap:       c.opts.Level2Cap,
			CleanupInterval: c.opts.CleanupTime,
			OnEvicted:       c.opts.OnEvicted,
		}

		// 创建存储实例
		c.store = store.NewStore(c.opts.CacheType, storeOpts)

		// 标记为已初始化
		atomic.StoreInt32(&c.initialized, 1)

		logrus.Infof("Cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
	}
}

// Add 向缓存中添加一个 key-value 对
func (c *Cache) Add(key string, value ByteView) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	if err := c.store.Set(key, value); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

// Get 从缓存中获取值，集成防穿透、击穿、雪崩逻辑
func (c *Cache) Get(ctx context.Context, key string) (value ByteView, ok bool) {
	// 1. 状态快速检查
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}
	// 如果缓存未初始化，直接返回未命中
	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	//c.mu.RLock()
	//defer c.mu.RUnlock()

	// 2. 【防穿透】第一道防线：布隆过滤器
	// 注意：布隆过滤器的查询不需要放在全局读锁内，它本身支持并发读
	if c.bf != nil && !c.bf.Contains(key) {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	// 3. 尝试从底层存储获取 (lru/lru2)
	// 移除了 c.mu.RLock()，因为 store.Get 内部已有分段锁或读写锁
	val, found := c.store.Get(key)
	if found {
		if bv, ok := val.(ByteView); ok {
			// 【关键】检查是否是之前存入的“空值占位符”
			if bv.b == nil {
				atomic.AddInt64(&c.misses, 1)
				return ByteView{}, false
			}
			atomic.AddInt64(&c.hits, 1)
			return bv, true
		}
	}

	// 4. 【防击穿】缓存未命中，调用 Singleflight 合并请求并回源
	return c.getLocally(ctx, key)
	//if !found {
	//	atomic.AddInt64(&c.misses, 1)
	//	return ByteView{}, false
	//}

	//// 更新命中计数
	//atomic.AddInt64(&c.hits, 1)
	//
	//// 转换并返回
	//if bv, ok := val.(ByteView); ok {
	//	return bv, true
	//}
	//
	//// 类型断言失败
	//logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
	//atomic.AddInt64(&c.misses, 1)
	//return ByteView{}, false
}

// AddWithExpiration 向缓存中添加一个带过期时间的 key-value 对
func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	// 计算过期时间
	//1. 获取基础过期时长
	expiration := time.Until(expirationTime)
	if expiration <= 0 {
		logrus.Debugf("Key %s already expired, not adding to cache", key)
		return
	}

	// 2. 引入随机抖动 (Jitter) 以防止雪崩
	// 产生一个基础时长 10% 以内的随机波动
	if expiration > time.Second { // 极短的过期时间不建议抖动
		jitterRange := int64(expiration) / 10
		// 加上或减去一个随机值，让过期时间点在 90%~110% 之间浮动
		jitter := time.Duration(rand.Int63n(jitterRange*2) - jitterRange)
		expiration += jitter
	}

	// 设置到底层存储
	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("Failed to add key %s to cache with expiration: %v", key, err)
	}
}

// Delete 从缓存中删除一个 key
func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Delete(key)
}

// Clear 清空缓存
func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()

	// 重置统计信息
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Len 返回缓存的当前存储项数量
func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Len()
}

// Close 关闭缓存，释放资源
func (c *Cache) Close() {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭底层存储
	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	// 重置缓存状态
	atomic.StoreInt32(&c.initialized, 0)

	logrus.Debugf("Cache closed, hits: %d, misses: %d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

// Stats 返回缓存统计信息
func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
	}

	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()

		// 计算命中率
		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}

func (c *Cache) getLocally(ctx context.Context, key string) (ByteView, bool) {
	// 使用 singleflight 确保并发下只有一个协程去加载数据
	viewi, err := c.sfg.Do(key, func() (interface{}, error) {
		// Double Check: 再次检查缓存，防止排队等待期间别人已经写好了
		if val, found := c.store.Get(key); found {
			return val, nil
		}

		// 5. 调用回源函数 (比如去查数据库)
		// 这里的 loader 对应你项目中的数据加载逻辑
		bytes, err := c.loader.Get(ctx, key)
		if err != nil {
			// 【处理穿透】数据库也没有，缓存一个空的 ByteView，TTL 设短一点 (1分钟)
			// 注意：这里调用你之前写的带抖动的 AddWithExpiration
			c.AddWithExpiration(key, ByteView{b: nil}, time.Now().Add(time.Minute))
			return nil, err
		}
		// 包装成 ByteView
		data := ByteView{b: bytes}

		// 6. 【防雪崩】写入缓存，AddWithExpiration 会自动计算随机抖动
		c.AddWithExpiration(key, data, time.Now().Add(c.opts.DefaultTTL))

		// 7. 同步更新布隆过滤器（如果是新产生的数据）
		if c.bf != nil {
			c.bf.Add(key)
		}

		return data, nil
	})

	if err != nil {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	atomic.AddInt64(&c.hits, 1)
	return viewi.(ByteView), true
}
