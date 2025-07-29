/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.common.util.concurrent.ReleasableLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DisposableIndexWriter {

    private final IndexWriter indexWriter;
    private final ReleasableLock readLock, writeLock;

    public DisposableIndexWriter(IndexWriter indexWriter, ReleasableLock readLock, ReleasableLock writeLock) {
        this.indexWriter = indexWriter;
        this.readLock = readLock;
        this.writeLock = writeLock;
    }

    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    public ReleasableLock getReadLock() {
        return readLock;
    }

    public ReleasableLock getWriteLock() {
        return writeLock;
    }
}
