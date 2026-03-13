package singleflight

import (
	"fmt"
	"sync"
)

// 代表正在进行或已结束的请求
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group manages all kinds of calls
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能
}

// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// Check if there is already an ongoing call for this key
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)
		c.wg.Wait()         // Wait for the existing request to finish
		return c.val, c.err // Return the result from the ongoing call
	}

	// If no ongoing request, create a new one
	c := &call{}
	c.wg.Add(1)
	g.m.Store(key, c) // Store the call in the map

	//// Execute the function and set the result
	//c.val, c.err = fn()
	//c.wg.Done() // Mark the request as done
	func() {
		defer func() {
			if r := recover(); r != nil {
				// 如果 fn() 崩溃了，也得 Done，否则所有等待的协程都会死锁
				c.err = fmt.Errorf("panic: %v", r)
			}
			c.wg.Done()
		}()
		c.val, c.err = fn()
	}()
	// After the request is done, clean up the map
	g.m.Delete(key)

	return c.val, c.err
}
