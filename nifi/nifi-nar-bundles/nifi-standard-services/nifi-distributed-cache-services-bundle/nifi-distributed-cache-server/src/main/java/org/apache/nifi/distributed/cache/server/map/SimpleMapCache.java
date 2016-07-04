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
package org.apache.nifi.distributed.cache.server.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.distributed.cache.server.EvictionPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMapCache implements MapCache {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMapCache.class);

    private final Map<ByteBuffer, MapCacheRecord> cache = new HashMap<>();
    private final SortedMap<MapCacheRecord, ByteBuffer> inverseCacheMap;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final String serviceIdentifier;

    private final int maxSize;

    public SimpleMapCache(final String serviceIdentifier, final int maxSize, final EvictionPolicy evictionPolicy) {
        // need to change to ConcurrentMap as this is modified when only the readLock is held
        inverseCacheMap = new ConcurrentSkipListMap<>(evictionPolicy.getComparator());
        this.serviceIdentifier = serviceIdentifier;
        this.maxSize = maxSize;
    }

    @Override
    public String toString() {
        return "SimpleSetCache[service id=" + serviceIdentifier + "]";
    }

    // don't need synchronized because this method is only called when the writeLock is held, and all
    // public methods obtain either the read or write lock
    private MapCacheRecord evict() {
        if (cache.size() < maxSize) {
            return null;
        }

        final MapCacheRecord recordToEvict = inverseCacheMap.firstKey();
        final ByteBuffer valueToEvict = inverseCacheMap.remove(recordToEvict);
        cache.remove(valueToEvict);

        if (logger.isDebugEnabled()) {
            logger.debug("Evicting value {} from cache", new String(valueToEvict.array(), StandardCharsets.UTF_8));
        }

        return recordToEvict;
    }

    @Override
    public MapPutResult putIfAbsent(final ByteBuffer key, final ByteBuffer value) {
        writeLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                // Record is null. We will add.
                final MapCacheRecord evicted = evict();
                final MapCacheRecord newRecord = new MapCacheRecord(key, value);
                cache.put(key, newRecord);
                inverseCacheMap.put(newRecord, key);

                if (evicted == null) {
                    return new MapPutResult(true, key, value, null, null, null);
                } else {
                    return new MapPutResult(true, key, value, null, evicted.getKey(), evicted.getValue());
                }
            }

            // Record is not null. Increment hit count and return result indicating that record was not added.
            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return new MapPutResult(false, key, value, record.getValue(), null, null);
        } finally {
            writeLock.unlock();
        }
    }


    @Override
    public MapPutResult put(final ByteBuffer key, final ByteBuffer value) {
        writeLock.lock();
        try {
            // evict if we need to in order to make room for a new entry.
            final MapCacheRecord evicted = evict();

            final MapCacheRecord record = new MapCacheRecord(key, value);
            final MapCacheRecord existing = cache.put(key, record);
            inverseCacheMap.put(record, key);

            final ByteBuffer existingValue = (existing == null) ? null : existing.getValue();
            final ByteBuffer evictedKey = (evicted == null) ? null : evicted.getKey();
            final ByteBuffer evictedValue = (evicted == null) ? null : evicted.getValue();

            return new MapPutResult(true, key, value, existingValue, evictedKey, evictedValue);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean containsKey(final ByteBuffer key) {
        readLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                return false;
            }

            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return true;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ByteBuffer get(final ByteBuffer key) {
        readLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                return null;
            }

            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return record.getValue();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ByteBuffer remove(ByteBuffer key) throws IOException {
        writeLock.lock();
        try {
            final MapCacheRecord record = cache.remove(key);
            if (record == null) {
                return null;
            }
            inverseCacheMap.remove(record);
            return record.getValue();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void shutdown() throws IOException {
    }
}
