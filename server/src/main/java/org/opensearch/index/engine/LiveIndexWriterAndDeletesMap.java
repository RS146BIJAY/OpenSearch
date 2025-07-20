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
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.KeyedLock;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;


/**
 * Maps _uid value to its deletes information. It also contains information on IndexWriter.
 *
 */
public class LiveIndexWriterAndDeletesMap implements ReferenceManager.RefreshListener {

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private static final class DeletesLookup {
        private final Map<BytesRef, DeleteEntry> lastDeleteEntryMap;
        private final Map<String, IndexWriter> criteriaBasedIndexWriterMap;

        private static final DeletesLookup EMPTY = new DeletesLookup(Collections.emptyMap(), Collections.emptyMap());

        private DeletesLookup(final Map<BytesRef, DeleteEntry> lastDeleteEntryMap, final Map<String, IndexWriter> criteriaBasedIndexWriterMap) {
            this.lastDeleteEntryMap = lastDeleteEntryMap;
            this.criteriaBasedIndexWriterMap = criteriaBasedIndexWriterMap;
        }

        void putLastDeleteEntryMap(BytesRef key, DeleteEntry deleteEntry) {
            lastDeleteEntryMap.put(key, deleteEntry);
        }

        void putCriteriaBasedIndexWriterMap(String criteria, IndexWriter indexWriter) {
            criteriaBasedIndexWriterMap.put(criteria, indexWriter);
        }

        DeleteEntry getDeleteEntryMap(BytesRef key) {
            return lastDeleteEntryMap.get(key);
        }

        IndexWriter getIndexWriter(String criteria) {
            return criteriaBasedIndexWriterMap.get(criteria);
        }

        int sizeOfDeleteEntryMap() {
            return lastDeleteEntryMap.size();
        }

        int sizeOfCriteriaBasedIndexWriterMap() {
            return criteriaBasedIndexWriterMap.size();
        }

        public DeleteEntry removeDeleteEntryMap(BytesRef uid) {
            return lastDeleteEntryMap.remove(uid);
        }

        public IndexWriter removeCriteriaBasedIndexWriterMap(String criteria) {
            return criteriaBasedIndexWriterMap.remove(criteria);
        }
    }

    /**
     * Map of version lookups
     *
     * @opensearch.internal
     */
    private static final class Maps {
        // All writes (adds and deletes) go into here:
        final DeletesLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes. We read from both current and old on lookup:
        final DeletesLookup old;

        Maps(DeletesLookup current, DeletesLookup old) {
            this.current = current;
            this.old = old;
        }

        Maps() {
            this(new DeletesLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(),
                ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency()), DeletesLookup.EMPTY);
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        Maps buildTransitionMap() {
//            System.out.println("Rotating deletion map with old map size " + current.lastDeleteEntrySetSize());
            return new Maps(
                new DeletesLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfDeleteEntryMap())
                    , ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfCriteriaBasedIndexWriterMap())),
                current
            );
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        Maps invalidateOldMap() {
            return new Maps(current, DeletesLookup.EMPTY);
        }

        void putLastDeleteEntryMap(BytesRef uid, DeleteEntry delete) {
            current.putLastDeleteEntryMap(uid, delete);
        }

        void removeDeleteEntryMap(BytesRef uid) {
            current.removeDeleteEntryMap(uid);
            if (old != DeletesLookup.EMPTY) {
                // we also need to remove it from the old map here to make sure we don't read this stale value while
                // we are in the middle of a refresh. Most of the time the old map is an empty map so we can skip it there.
                old.removeDeleteEntryMap(uid);
            }
        }
    }

    private volatile Maps maps = new Maps();

    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map. While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        maps = maps.buildTransitionMap();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        maps = maps.invalidateOldMap();
    }

    /**
     * Returns the live version (add or delete) for this uid.
     */
    DeleteEntry getUnderLock(final BytesRef uid) {
        return getUnderLock(uid, maps);
    }

    private DeleteEntry getUnderLock(final BytesRef uid, Maps currentMaps) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        // First try to get the "live" value:
        DeleteEntry value = currentMaps.current.getDeleteEntryMap(uid);
        if (value != null) {
            return value;
        }

        return currentMaps.old.getDeleteEntryMap(uid);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }
}
