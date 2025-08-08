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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.KeyedLock;
import org.opensearch.common.util.concurrent.ReleasableLock;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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

    public static final class DisposableIndexWriter {

        private final IndexWriter indexWriter;
        private final CriteriaBasedIndexWriterLookup lookupMap;

        public DisposableIndexWriter(IndexWriter indexWriter, CriteriaBasedIndexWriterLookup lookupMap) {
            this.indexWriter = indexWriter;
            this.lookupMap = lookupMap;

        }

        public IndexWriter getIndexWriter() {
            return indexWriter;
        }

        public CriteriaBasedIndexWriterLookup getLookupMap() {
            return lookupMap;
        }
    }

    public static final class CriteriaBasedIndexWriterLookup {
        private final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap;
        private final Map<BytesRef, DeleteEntry> lastDeleteEntrySet;
        private final Map<BytesRef, String> criteria;
        private final ReentrantReadWriteLock mapLock;
        private final ReleasableLock mapReadLock;
        private final ReleasableLock mapWriteLock;
        private final long version;

        private static final CriteriaBasedIndexWriterLookup EMPTY = new CriteriaBasedIndexWriterLookup(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), 0);

        private CriteriaBasedIndexWriterLookup(final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap, Map<BytesRef, DeleteEntry> lastDeleteEntrySet, Map<BytesRef, String> criteria, long version) {
            this.criteriaBasedIndexWriterMap = criteriaBasedIndexWriterMap;
            this.lastDeleteEntrySet = lastDeleteEntrySet;
            this.mapLock = new ReentrantReadWriteLock();
            this.mapReadLock = new ReleasableLock(mapLock.readLock());
            this.mapWriteLock = new ReleasableLock(mapLock.writeLock());
            this.criteria = criteria;
            this.version = version;
        }

//        void putCriteriaBasedIndexWriterMap(String key, DisposableIndexWriter writer) {
//            criteriaBasedIndexWriterMap.put(key, writer);
//        }

        DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(String criteria,
                                                                    CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) {
            mapReadLock.acquire();
            return criteriaBasedIndexWriterMap.computeIfAbsent(criteria, (key) -> {
                try {
                    return indexWriterSupplier.apply(criteria, this);
                } catch (IOException e) {
                    throw new OpenSearchException(e);
                }
            });
        }

        DisposableIndexWriter getIndexWriterForCriteria(String criteria) {
            return criteriaBasedIndexWriterMap.get(criteria);
        }

        int sizeOfCriteriaBasedIndexWriterMap() {
            return criteriaBasedIndexWriterMap.size();
        }

        int sizeOfLastDeleteEntrySet() {
            return lastDeleteEntrySet.size();
        }

        int sizeOfCriteria() {
            return criteria.size();
        }

        DeleteEntry getLastDeleteEntry(BytesRef key) {
            return lastDeleteEntrySet.get(key);
        }

        void putLastDeleteEntry(BytesRef uid, DeleteEntry deleteEntry) {
            lastDeleteEntrySet.put(uid, deleteEntry);
        }

        void putCriteriaForDoc(BytesRef key, String criteria) {
            this.criteria.put(key, criteria);
        }

        String getCriteriaForDoc(BytesRef key) {
            return criteria.get(key);
        }

        void removeLastDeleteEntry(BytesRef key) {
            lastDeleteEntrySet.remove(key);
        }

        public ReleasableLock getMapReadLock() {
            return mapReadLock;
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
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(), ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(), 0),
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
                            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet()),
                        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet()), current.version + 1),
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

        void putLastDeleteEntryInCurrentMap(BytesRef uid, DeleteEntry deleteEntry) {
            current.putLastDeleteEntry(uid, deleteEntry);
        }

        void putLastDeleteEntryInOldMap(BytesRef bytesRef, DeleteEntry deleteEntry) {
            old.putLastDeleteEntry(bytesRef, deleteEntry);
        }

        void removeLastDeleteEntryInCurrentMap(BytesRef uid) {
            current.removeLastDeleteEntry(uid);
        }

        void removeLastDeleteEntryInOldMap(BytesRef uid) {
            old.removeLastDeleteEntry(uid);
        }

        void putCriteriaForDoc(BytesRef key, String criteria) {
            current.putCriteriaForDoc(key, criteria);
        }

        String getCriteriaForDoc(BytesRef key) {
            return current.getCriteriaForDoc(key);
//            if (criteria == null) {
//                criteria = old.getCriteriaForDoc(key);
//            }

//            return criteria;
        }

        DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(String criteria,
                                                                    CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) {
//            System.out.println("Before map, Live version map size " + current.sizeOfCriteriaBasedIndexWriterMap() + " old size " + old.sizeOfCriteriaBasedIndexWriterMap() + " maps " + this);
            return current.computeIndexWriterIfAbsentForCriteria(criteria, indexWriterSupplier);
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
//        System.out.println("Before build transition maps " + maps + " with current " + maps.current.sizeOfCriteriaBasedIndexWriterMap() + " and old " + maps.old.sizeOfCriteriaBasedIndexWriterMap());

        maps = maps.buildTransitionMap();
        try(Releasable ignore = maps.old.mapWriteLock.acquire()) {
//            System.out.println("build transition maps " + maps + " with current " + maps.current.sizeOfCriteriaBasedIndexWriterMap() + " and old " + maps.old.sizeOfCriteriaBasedIndexWriterMap());
        }
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
//        System.out.println("After refresh taking lock on " + maps + " with lock " + maps.old.mapWriteLock);
        maps = maps.invalidateOldMap();
//        System.out.println("Invalidated maps " + maps + " with current " + maps.current.sizeOfCriteriaBasedIndexWriterMap() + " and old " + maps.old.sizeOfCriteriaBasedIndexWriterMap());
    }

    public ReleasableLock getCurrentCriteriaBasedIndexWriterLookupReadLock() {
        return maps.current.mapReadLock;
    }

    public ReleasableLock getOldCriteriaBasedIndexWriterLookupReadLock() {
        return maps.old.mapReadLock;
    }

    public ReleasableLock getOldCriteriaBasedIndexWriterLookupWriteLock() {
        return maps.old.mapReadLock;
    }

    public CriteriaBasedIndexWriterLookup getOldCriteriaBasedIndexWriterLookup() {
        return maps.old;
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
//        assert assertKeyedLockHeldByCurrentThread(uid);
//        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.putLastDeleteEntryInCurrentMap(uid, entry);
    }

//    void putLastDeleteEntryUnderLockInOldMap(BytesRef uid, DeleteEntry entry) {
//        assert assertKeyedLockHeldByCurrentThread(uid);
//        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
//        maps.putLastDeleteEntryInOldMap(uid, entry);
//    }

    void removeLastDeleteEntryUnderLockInOldMap(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.removeLastDeleteEntryInOldMap(uid);
    }

    void removeLastDeleteEntryUnderLockInCurrentMap(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.removeLastDeleteEntryInCurrentMap(uid);
    }

    void putCriteria(BytesRef uid, String criteria) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.putCriteriaForDoc(uid, criteria);
    }

    DisposableIndexWriter getIndexWriterForIdFromCurrent(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        return getIndexWriterForIdFromCurrent(uid, maps.current);
    }

    // Avoid the issue of write lock getting applied on a separate map due to map getting rotated.
    DisposableIndexWriter getIndexWriterForIdFromCurrent(BytesRef uid, CriteriaBasedIndexWriterLookup currentMaps) {
        currentMaps.mapReadLock.acquire();
        String criteria = getCriteriaForDoc(uid);
        if (criteria != null) {
            DisposableIndexWriter disposableIndexWriter = currentMaps.getIndexWriterForCriteria(criteria);
            if (disposableIndexWriter != null) {
                return disposableIndexWriter;
            }
        }

        currentMaps.mapReadLock.close();
        return null;
    }

    DisposableIndexWriter getIndexWriterForIdFromOld(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        String criteria = getCriteriaForDoc(uid);
        if (criteria != null) {
            return maps.old.getIndexWriterForCriteria(criteria);
        }

        return null;
    }

    String getCriteriaForDoc(BytesRef uid) {
        return maps.getCriteriaForDoc(uid);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }

    DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(final String criteria,
                                                      CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) throws IOException {
//        System.out.println("Get and lock taking lock on " + maps + " with lock " + maps.current.mapReadLock);
        return computeIndexWriterIfAbsentForCriteria(criteria, maps, indexWriterSupplier);
    }

    DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(final String criteria, Maps currentMaps,
                                                                CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) {
//        System.out.println("Before live version map, Live version map size " + currentMaps.sizeOfCurrentCriteriaBasedIndexWriterMap() + " old size " + currentMaps.sizeOfOldCriteriaBasedIndexWriterMap() + " maps " + currentMaps);
        return currentMaps.computeIndexWriterIfAbsentForCriteria(criteria, indexWriterSupplier);
    }

    public Collection<DisposableIndexWriter> getIndexWritersMarkForRefresh() {
         return new HashSet<>(maps.old.criteriaBasedIndexWriterMap.values());
    }

    public Map<String, DisposableIndexWriter> getMarkForRefreshIndexWriterMap() {
        return maps.old.criteriaBasedIndexWriterMap;
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
            .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());;
        for (IndexWriter currentWriter : currentWriterSet) {
            flushingBytes += currentWriter.getPendingNumDocs();
        }

        return flushingBytes;
    }

    public boolean hasUncommittedChanges() {
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values()
            .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());;
        for (IndexWriter currentWriter : currentWriterSet) {
            if (currentWriter.hasUncommittedChanges()) {
                return true;
            }
        }

        return false;
    }

    public Throwable getTragicException() {
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values().stream()
            .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter writer: currentWriterSet) {
            if (writer.isOpen() == false && writer.getTragicException() != null) {
                return writer.getTragicException();
            }
        }

        Collection<IndexWriter> oldWriterSet = maps.old.criteriaBasedIndexWriterMap.values()
            .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());;
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
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values().stream()
            .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());

        try(ReleasableLock ignore = maps.current.mapWriteLock.acquire()) {
            for (IndexWriter indexWriter : currentWriterSet) {
                if (indexWriter.isOpen() == true) {
                    indexWriter.rollback();
                }
            }
        }

        Collection<IndexWriter> oldWriterSet = maps.old.criteriaBasedIndexWriterMap.values().stream()
            .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        try(ReleasableLock ignore = maps.old.mapWriteLock.acquire()) {
            for (IndexWriter indexWriter : oldWriterSet) {
                if (indexWriter.isOpen() == true) {
                    indexWriter.rollback();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        Collection<IndexWriter> currentWriterSet = maps.current.criteriaBasedIndexWriterMap.values().stream()
            .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());

        try(ReleasableLock ignore = maps.current.mapWriteLock.acquire()) {
            for (IndexWriter indexWriter : currentWriterSet) {
                if (indexWriter.isOpen() == true) {
                    indexWriter.close();
                }
            }
        }

        Collection<IndexWriter> oldWriterSet = maps.old.criteriaBasedIndexWriterMap.values().stream()
            .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        try(ReleasableLock ignore = maps.old.mapWriteLock.acquire()) {
            for (IndexWriter indexWriter : oldWriterSet) {
                if (indexWriter.isOpen() == true) {
                    indexWriter.close();
                }
            }
        }
    }
}
