/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.KeyedLock;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;

public class DeletedDocumentsMap implements ReferenceManager.RefreshListener {
    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();
    private volatile Maps maps = new Maps();

    private static final class DeletedDocumentsMapLookup {
        private final Map<BytesRef, DeleteEntry> termsMap;
        private static final DeletedDocumentsMapLookup EMPTY = new DeletedDocumentsMapLookup(Collections.emptyMap());

        private DeletedDocumentsMapLookup(final Map<BytesRef, DeleteEntry> termsMap) {
            this.termsMap = termsMap;
        }

        Map<BytesRef, DeleteEntry> getTermsMap() {
            return termsMap;
        }

        void putTermEntry(BytesRef key, DeleteEntry deleteEntry) {
            System.out.println("Adding to delete map: " + deleteEntry);
            termsMap.put(key, deleteEntry);
        }

        boolean isEmpty() {
            return termsMap.isEmpty();
        }

        int size() {
            return termsMap.size();
        }

        public DeleteEntry remove(BytesRef uid) {
            return termsMap.remove(uid);
        }
    }

    private static final class Maps {
        final DeletedDocumentsMapLookup current;

        final DeletedDocumentsMapLookup old;

        Maps(DeletedDocumentsMapLookup current, DeletedDocumentsMapLookup old) {
            this.current = current;
            this.old = old;
        }

        Maps() {
            this(new DeletedDocumentsMapLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency()), DeletedDocumentsMapLookup.EMPTY);
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        Maps buildTransitionMap() {
            return new Maps(
                new DeletedDocumentsMapLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.size())),
                current
            );
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        Maps invalidateOldMap() {
            return new Maps(current, DeletedDocumentsMapLookup.EMPTY);
        }

        void put(BytesRef uid, DeleteEntry deleteEntry) {
            current.putTermEntry(uid, deleteEntry);
        }

        Map<BytesRef, DeleteEntry> getTermsDuringRefresh() {
            return old.getTermsMap();
        }
    }

    @Override
    public void beforeRefresh() throws IOException {
        maps = maps.buildTransitionMap();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        maps = maps.invalidateOldMap();
    }

    void addUnderLock(BytesRef uid, DeleteEntry deleteEntry) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.put(uid, deleteEntry);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }

    Map<BytesRef, DeleteEntry> getTermsDuringRefresh() {
        return maps.getTermsDuringRefresh();
    }

    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }
}
