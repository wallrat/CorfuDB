package org.corfudb.util;

import lombok.Data;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Created by box on 1/9/18.
 */
public class VLOLRU {

    Map<Long, CacheEntry> map = new ConcurrentHashMap();

    ReadWriteLock rwLock = new ReentrantReadWriteLock();

    AtomicLong currentTimestamp = new AtomicLong(0l);

    int cacheSize;

    public VLOLRU(int size) {
        this.cacheSize = size;
    }

    public void invalidateAll() {
        try {
            rwLock.writeLock().lock();
            map.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    void checkGC() {
        if (map.size() > cacheSize * 2) {

            try {
                rwLock.writeLock().lock();
                long window = currentTimestamp.get() - cacheSize;
                map.forEach((key, cacheEntry) -> {
                    if (cacheEntry.getTimestamp() < window) {
                        map.remove(key);
                    }
                });
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    void addToCache(@Nonnull Long key, @Nonnull List<SMREntry> val) {
        checkGC();
        map.put(key, new CacheEntry(currentTimestamp.getAndIncrement(), val));
    }

    @CheckForNull
    public List<SMREntry> get(@Nonnull Long key) {
        CacheEntry entry = map.get(key);
        if (entry == null) {
            return null;
        } else {
            try {
                rwLock.readLock().lock();
                // Cache size doesn't change, just update the timestamp
                // TODO(Maithem) Don't generate new cache entries on every read
                map.put(key, new CacheEntry(currentTimestamp.getAndIncrement(), entry.getVal()));
                return entry.getVal();
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    public void put(@Nonnull Long key, @Nonnull List<SMREntry> val) {
        addToCache(key, val);
    }

    @Data
    class CacheEntry {
        final long timestamp;
        final List<SMREntry> val;
    }
}
