/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.KeyedLock;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class DeleteEntryMap implements ReferenceManager.RefreshListener {
    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private static final class DeleteEntryLookup {
        private final Map<BytesRef, DeleteEntry> lastDeleteEntrySet;
        private static final DeleteEntryLookup EMPTY = new DeleteEntryLookup(Collections.emptyMap());

        private DeleteEntryLookup(Map<BytesRef, DeleteEntry> lastDeleteEntrySet) {
            this.lastDeleteEntrySet = lastDeleteEntrySet;
        }

        DeleteEntry get(BytesRef key) {
            return lastDeleteEntrySet.get(key);
        }

        void putLastDeleteEntry(BytesRef key, DeleteEntry deleteEntry) {
            lastDeleteEntrySet.put(key, deleteEntry);
        }

        int size() {
            return lastDeleteEntrySet.size();
        }

        public DeleteEntry removeDeleteEntry(BytesRef key) {
            return lastDeleteEntrySet.remove(key);
        }
    }

    private static final class Maps {
        // All writes (adds and deletes) go into here:
        final DeleteEntryLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes. We read from both current and old on lookup:
        final DeleteEntryLookup old;

        Maps(DeleteEntryLookup current, DeleteEntryLookup old) {
            this.current = current;
            this.old = old;
        }

        Maps() {
            this(new DeleteEntryLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency()), DeleteEntryLookup.EMPTY);
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        Maps buildTransitionMap() {
            System.out.println("Rotating deletion map with old map size " + current.size());
            return new Maps(
                new DeleteEntryLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.size())),
                current
            );
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        Maps invalidateOldMap() {
            return new Maps(current, DeleteEntryLookup.EMPTY);
        }

        void putLastDeleteEntry(BytesRef uid, DeleteEntry deleteEntry) {
            current.putLastDeleteEntry(uid, deleteEntry);
        }
    }

    private volatile Maps maps = new Maps();

    @Override
    public void beforeRefresh() throws IOException {
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
        DeleteEntry value = currentMaps.current.get(uid);
        if (value != null) {
            return value;
        }

        return currentMaps.old.get(uid);
    }

    void putDeleteEntryUnderLock(BytesRef uid, DeleteEntry entry) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        if (entry != null && entry.getVersionOfDeletion() > 0) {
            System.out.println("Adding delete entry " + entry);
            maps.putLastDeleteEntry(uid, entry);
        }
    }

    Map<BytesRef, DeleteEntry> getAllOld() {
        return maps.old.lastDeleteEntrySet;
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }

    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }
}
