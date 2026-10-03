package trace

import "time"

// runReaper 定期回收过期链路，与采集、查询并发运行。
func (c *Collector) runReaper() {
	ticker := time.NewTicker(c.cfg.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.reapOnce()
		}
	}
}

// reapOnce 执行一轮回收，并把删除记录持久化。
func (c *Collector) reapOnce() {
	removed := c.store.reap(time.Now())
	if len(removed) > 0 && c.persister != nil {
		for _, id := range removed {
			_ = c.persister.AppendDelete(id)
		}
		_ = c.persister.Flush()
	}
	// WAL 超阈值时触发快照压缩。
	if c.persister != nil {
		_ = c.persister.maybeCompact(c.store)
	}
}

// ReapNow 立即执行一轮回收（主要用于测试）。
func (c *Collector) ReapNow() {
	c.reapOnce()
}
