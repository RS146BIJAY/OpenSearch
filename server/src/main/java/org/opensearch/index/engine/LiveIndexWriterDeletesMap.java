/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.KeyedLock;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.DisposableIndexWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


/**
 * Maps _uid value to its deletes information. It also contains information on IndexWriter.
 *
 */
public class LiveIndexWriterDeletesMap implements ReferenceManager.RefreshListener, Closeable {

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private EngineConfig engineConfig;
    public LiveIndexWriterDeletesMap(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    private static final class CriteriaBasedIndexWriterLookup {
        private final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap;
        private final Map<BytesRef, DeleteEntry> lastDeleteEntrySet;
        final ReentrantReadWriteLock mapLock;
        final ReleasableLock mapReadLock;
        final ReleasableLock mapWriteLock;

        private static final CriteriaBasedIndexWriterLookup EMPTY = new CriteriaBasedIndexWriterLookup(Collections.emptyMap(), Collections.emptyMap());

        private CriteriaBasedIndexWriterLookup(final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap, Map<BytesRef, DeleteEntry> lastDeleteEntrySet) {
            this.criteriaBasedIndexWriterMap = criteriaBasedIndexWriterMap;
            this.lastDeleteEntrySet = lastDeleteEntrySet;
            this.mapLock = new ReentrantReadWriteLock();
            this.mapReadLock = new ReleasableLock(mapLock.readLock());
            this.mapWriteLock = new ReleasableLock(mapLock.writeLock());
        }

//        void putCriteriaBasedIndexWriterMap(String key, DisposableIndexWriter writer) {
//            criteriaBasedIndexWriterMap.put(key, writer);
//        }

        DisposableIndexWriter getCriteriaBasedIndexWriter(String criteria, CheckedTriFunction<String, ReleasableLock, ReleasableLock, DisposableIndexWriter, IOException> indexWriterSupplier) {
            mapReadLock.acquire();
            return criteriaBasedIndexWriterMap.computeIfAbsent(criteria, (key) -> {
                try {
                    return indexWriterSupplier.apply(criteria, mapReadLock, mapWriteLock);
                } catch (IOException e) {
                    throw new OpenSearchException(e);
                }
            });
        }

        int sizeOfCriteriaBasedIndexWriterMap() {
            return criteriaBasedIndexWriterMap.size();
        }

        int sizeOfLastDeleteEntrySet() {
            return lastDeleteEntrySet.size();
        }

        DeleteEntry getLastDeleteEntry(BytesRef key) {
            return lastDeleteEntrySet.get(key);
        }

        void putLastDeleteEntry(BytesRef key, DeleteEntry deleteEntry) {
            lastDeleteEntrySet.put(key, deleteEntry);
        }
    }

    /**
     * Map of version lookups
     *
     * @opensearch.internal
     */
    private final class Maps {
        // All writes (adds and deletes) go into here:
        final CriteriaBasedIndexWriterLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes. We read from both current and old on lookup:
        final CriteriaBasedIndexWriterLookup old;

        Maps(CriteriaBasedIndexWriterLookup current, CriteriaBasedIndexWriterLookup old) {
            this.current = current;
            this.old = old;
        }

        Maps() {
            this(new CriteriaBasedIndexWriterLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(),
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency()),
                CriteriaBasedIndexWriterLookup.EMPTY);
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        Maps buildTransitionMap() {
//            System.out.println("Rotating deletion map with old map " + this + " size " + current.sizeOfLastDeleteEntrySet() + " writer of size " + current.sizeOfCriteriaBasedIndexWriterMap());
//            System.out.println("Inside buildTransitionMap for " + this + " for current " + current + " lock: " + current.mapReadLock + " " + current.mapWriteLock + " and old " + old + " lock: " + old.mapReadLock + " " + old.mapWriteLock);

            // This ensures writer map is not rotated during the time when we are obtaining an IndexWriter from map. As
            // this may cause updates to go out of sync with current IndexWriter.
            return new Maps(
                    new CriteriaBasedIndexWriterLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfCriteriaBasedIndexWriterMap()),
                            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet())),
                    current
            );
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        Maps invalidateOldMap() {
//            System.out.println("Inside invalidateOldMap for " + this + " for current " + current + " lock: " + current.mapReadLock + " " + current.mapWriteLock + " and old " + old + " lock: " + old.mapReadLock + " " + old.mapWriteLock);
            return new Maps(current, CriteriaBasedIndexWriterLookup.EMPTY);
        }

//        void putCriteriaBasedIndexWriterMap(final String criteria, final DisposableIndexWriter writer) {
//            current.putCriteriaBasedIndexWriterMap(criteria, writer);
//        }

        void putLastDeleteEntryInCurrentMap(BytesRef uid, DeleteEntry deleteEntry) {
            current.putLastDeleteEntry(uid, deleteEntry);
        }

        void putLastDeleteEntryInOldMap(BytesRef uid, DeleteEntry deleteEntry) {
            old.putLastDeleteEntry(uid, deleteEntry);
        }

        DisposableIndexWriter getCriteriaBasedIndexWriter(String criteria, CheckedTriFunction<String, ReleasableLock, ReleasableLock, DisposableIndexWriter, IOException> indexWriterSupplier) {
//            System.out.println("Before map, Live version map size " + current.sizeOfCriteriaBasedIndexWriterMap() + " old size " + old.sizeOfCriteriaBasedIndexWriterMap() + " maps " + this);
            return current.getCriteriaBasedIndexWriter(criteria, indexWriterSupplier);
//            System.out.println("In map, Live version map size " + current.sizeOfCriteriaBasedIndexWriterMap() + " old size " + old.sizeOfCriteriaBasedIndexWriterMap() + " maps " + this);
        }

        int sizeOfCurrentCriteriaBasedIndexWriterMap() {
            return current.sizeOfCriteriaBasedIndexWriterMap();
        }

        int sizeOfOldCriteriaBasedIndexWriterMap() {
            return old.sizeOfCriteriaBasedIndexWriterMap();
        }
    }

    private volatile Maps maps = new Maps();

    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map. While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:

//        System.out.println("Before build transition maps " + maps + " with current " + maps.current.sizeOfCriteriaBasedIndexWriterMap() + " and old " + maps.old.sizeOfCriteriaBasedIndexWriterMap());

        maps = maps.buildTransitionMap();
        try(Releasable ignore = maps.old.mapWriteLock.acquire()) {
//            System.out.println("build transition maps " + maps + " with current " + maps.current.sizeOfCriteriaBasedIndexWriterMap() + " and old " + maps.old.sizeOfCriteriaBasedIndexWriterMap());
        }

        // All old writers are added here.
//        maps.old.criteriaBasedIndexWriterMap.values().forEach(disposableIndexWriter -> {
//            disposableIndexWriter.setMarkForFlush(true);
//        });
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
//        System.out.println("After refresh taking lock on " + maps + " with lock " + maps.old.mapWriteLock);
//        System.out.println("Invalidated maps " + maps + " with current " + maps.current.sizeOfCriteriaBasedIndexWriterMap() + " and old " + maps.old.sizeOfCriteriaBasedIndexWriterMap());
        maps = maps.invalidateOldMap();
    }

    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    DeleteEntry getLastDeleteEntryUnderLock(final BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        Maps currentMaps = maps;
        DeleteEntry deleteEntry = currentMaps.current.getLastDeleteEntry(uid);
        if (deleteEntry != null) {
            return deleteEntry;
        }

        deleteEntry = currentMaps.old.getLastDeleteEntry(uid);
        return deleteEntry;
    }

    public Map<BytesRef, DeleteEntry> getLastDeleteEntrySet() {
        return maps.old.lastDeleteEntrySet;
    }

    void putLastDeleteEntryUnderLockInNewMap(BytesRef uid, DeleteEntry entry) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.putLastDeleteEntryInCurrentMap(uid, entry);
    }

    void putLastDeleteEntryUnderLockInOldMap(BytesRef uid, DeleteEntry entry) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.putLastDeleteEntryInOldMap(uid, entry);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }

    DisposableIndexWriter getAndLock(final String criteria, CheckedTriFunction<String, ReleasableLock, ReleasableLock, DisposableIndexWriter, IOException> indexWriterSupplier) throws IOException {
//        System.out.println("Get and lock taking lock on " + maps + " with lock " + maps.current.mapReadLock);
        return getAndLock(criteria, maps, indexWriterSupplier);
    }

    DisposableIndexWriter getAndLock(final String criteria, Maps currentMaps, CheckedTriFunction<String, ReleasableLock, ReleasableLock, DisposableIndexWriter, IOException> indexWriterSupplier) throws IOException {
//        System.out.println("Before live version map, Live version map size " + currentMaps.sizeOfCurrentCriteriaBasedIndexWriterMap() + " old size " + currentMaps.sizeOfOldCriteriaBasedIndexWriterMap() + " maps " + currentMaps);
        return currentMaps.getCriteriaBasedIndexWriter(criteria, indexWriterSupplier);
    }

    public Collection<DisposableIndexWriter> getIndexWritersMarkForRefresh() {
         return new HashSet<>(maps.old.criteriaBasedIndexWriterMap.values());
    }

    public long getFlushingBytes() {
        long flushingBytes = 0;
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values()
                .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter currentWriter : currentWriterSet) {
            flushingBytes += currentWriter.getFlushingBytes();
        }

        return flushingBytes;
    }

    public long getPendingNumDocs() {
        long flushingBytes = 0;
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values()
                .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter currentWriter : currentWriterSet) {
            flushingBytes += currentWriter.getPendingNumDocs();
        }

        return flushingBytes;
    }

    public boolean hasUncommittedChanges() {
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values()
                .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter currentWriter : currentWriterSet) {
            if (currentWriter.hasUncommittedChanges()) {
                return true;
            }
        }

        return false;
    }

    public Throwable getTragicException() {
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values()
                .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter writer: currentWriterSet) {
            if (writer.isOpen() == false && writer.getTragicException() != null) {
                return writer.getTragicException();
            }
        }

        Collection<IndexWriter> oldWriterSet = maps.old.criteriaBasedIndexWriterMap.values()
                .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter writer: oldWriterSet) {
            if (writer.isOpen() == false && writer.getTragicException() != null) {
                return writer.getTragicException();
            }
        }

        return null;
    }

    public int getLastDeleteEntrySize() {
        return maps.old.lastDeleteEntrySet.size();
    }

    public int getMarkForRefreshIndexWriterSize() {
        return maps.sizeOfOldCriteriaBasedIndexWriterMap();
    }

    public int getCurrentSize() {
        return maps.sizeOfCurrentCriteriaBasedIndexWriterMap();
    }

    // TODO: Fix rollback scenarioes (Remove docs from child and do a rollback from Parent IndexWriter)
    public void rollbackActiveIndexWriter() throws IOException {

        try(ReleasableLock ignore = maps.current.mapWriteLock.acquire()) {
            for (DisposableIndexWriter disposableIndexWriter : maps.current.criteriaBasedIndexWriterMap.values()) {
                IndexWriter indexWriter = disposableIndexWriter.getIndexWriter();
                if (indexWriter.isOpen() == true) {
                    indexWriter.rollback();
                }
            }
        }

        try(ReleasableLock ignore = maps.old.mapWriteLock.acquire()) {
            for (DisposableIndexWriter disposableIndexWriter : maps.old.criteriaBasedIndexWriterMap.values()) {
                IndexWriter indexWriter = disposableIndexWriter.getIndexWriter();
                if (indexWriter.isOpen() == true) {
                    indexWriter.rollback();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try(ReleasableLock ignore = maps.current.mapWriteLock.acquire()) {
            for (DisposableIndexWriter disposableIndexWriter : maps.current.criteriaBasedIndexWriterMap.values()) {
                IndexWriter indexWriter = disposableIndexWriter.getIndexWriter();
                if (indexWriter.isOpen() == true) {
                    indexWriter.close();
                }
            }
        }

        try(ReleasableLock ignore = maps.old.mapWriteLock.acquire()) {
            for (DisposableIndexWriter disposableIndexWriter : maps.old.criteriaBasedIndexWriterMap.values()) {
                IndexWriter indexWriter = disposableIndexWriter.getIndexWriter();
                if (indexWriter.isOpen() == true) {
                    indexWriter.close();
                }
            }
        }
    }
}
