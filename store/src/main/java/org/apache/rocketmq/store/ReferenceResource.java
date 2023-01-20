/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

/**
 * MappedFile的父类
 * 从什么角度去理解这个类的设计呢
 * 资源的引用
 */
public abstract class ReferenceResource {
    /**
     * 被引用次数
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    /**
     * 是否可用（未被销毁）
     */
    protected volatile boolean available = true;

    /**
     * 是否已经被清除
     */
    protected volatile boolean cleanupOver = false;

    /**
     * 第一次关闭的时间戳
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 笔记
     * 不是很懂这里的逻辑  为什么这里要这样设计？
     *
     * 每当持有资源时，引用数加1，
     * 如果发现已经不可用就回退，这里用双层检验保证线程安全
     * 1.isAvailable()
     * 2.this.refCount.getAndIncrement() > 0
     * @return
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭
     * 主动触发销毁过程，实际上会调用release函数来进行销毁
     * 如果这里销毁失败，会在每次尝试销毁时，按照一定的时间间隔，将引用数-1000来强制进行销毁
     *
     * @param intervalForcibly  拒绝被销毁的最大存活时间
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放资源，如果以引用数小于0，则开始销毁
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
