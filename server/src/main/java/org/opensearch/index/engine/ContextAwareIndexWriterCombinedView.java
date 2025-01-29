/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.index.OpenSearchMultiReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.ParseContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ContextAwareIndexWriterCombinedView implements Closeable {

    private final Map<String, OpenSearchConcurrentMergeScheduler> mergeSchedulerCriteriaMap;
    private final Map<String, CombinedDeletionPolicy> childLevelCombinedDeletionPolicyMap;
    private final Map<String, IndexWriter> criteriaBasedIndexWriters;
    private final SegmentInfos combinedSegmentInfos;
    private final ShardId shardId;
    private LiveIndexWriterConfig indexWriterConfig = null;
    private Directory directory;

    public ContextAwareIndexWriterCombinedView(final Map<String, OpenSearchConcurrentMergeScheduler> mergeSchedulerCriteriaMap,
                                               final Map<String, CombinedDeletionPolicy> childLevelCombinedDeletionPolicyMap,
                                               final Map<String, IndexWriter> criteriaBasedIndexWriters,
                                               final Directory directory, final ShardId shardId) throws IOException {
        this.mergeSchedulerCriteriaMap = mergeSchedulerCriteriaMap;
        this.childLevelCombinedDeletionPolicyMap = childLevelCombinedDeletionPolicyMap;
        this.criteriaBasedIndexWriters = criteriaBasedIndexWriters;
        this.directory = directory;
        this.combinedSegmentInfos = Lucene.combineSegmentInfos(criteriaBasedIndexWriters, directory);
        this.shardId = shardId;
        for (IndexWriter writer: criteriaBasedIndexWriters.values()) {
            this.indexWriterConfig = writer.getConfig();
            break;
        }
    }

    public Map<String, String> getUserData() {
        return combinedSegmentInfos.getUserData();
    }

    public OpenSearchMultiReader openMultiReader() throws IOException {
        final List<IndexReader> readerList = new ArrayList<>();
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            final OpenSearchDirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(
                    DirectoryReader.open(indexWriter),
                    shardId
            );

            readerList.add(directoryReader);
        }

        return new OpenSearchMultiReader(readerList, directory, criteriaBasedIndexWriters.keySet(), shardId);
    }

    public void rollback() {
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            try {
                indexWriter.rollback();
            } catch (IOException inner) { // iw is closed below

            }
        }
    }


    @Override
    public void close() throws IOException {
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            indexWriter.close();
        }
    }

    public boolean hasSnapshottedCommits() {
        for (CombinedDeletionPolicy combinedDeletionPolicy: childLevelCombinedDeletionPolicyMap.values()) {
            if (combinedDeletionPolicy.hasSnapshottedCommits()) {
                return true;
            }
        }

        return false;
    }

    public long getFlushingBytes() {
        long flushingBytes = 0;
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            flushingBytes += indexWriter.getFlushingBytes();
        }

        return flushingBytes;
    }

    public Throwable getTragicException() {
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            if (indexWriter.getTragicException() != null) {
                return indexWriter.getTragicException();
            }
        }

        return null;
    }

    public long getPendingNumDocs() {
        long pendingNumDocsCount = 0;
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            pendingNumDocsCount += indexWriter.getPendingNumDocs();
        }

        return pendingNumDocsCount;
    }

    public LiveIndexWriterConfig getConfig() {
        return indexWriterConfig;
    }

    public long ramBytesUsed() {
        long ramBytesUsed = 0;
        for (IndexWriter indexWriter: criteriaBasedIndexWriters.values()) {
            ramBytesUsed += indexWriter.ramBytesUsed();
        }

        return ramBytesUsed;
    }

//    public IndexCommit acquireIndexCommit() {
//
//    }

    private List<IndexCommit> getLastIndexCommits(boolean acquireSafeCommit) throws IOException {
        final List<IndexCommit> indexCommits = new ArrayList<>();
        for (Map.Entry<String, CombinedDeletionPolicy> entry: childLevelCombinedDeletionPolicyMap.entrySet()) {
            CombinedDeletionPolicy combinedDeletionPolicy = entry.getValue();
            final IndexCommit lastCommit = combinedDeletionPolicy.acquireIndexCommit(acquireSafeCommit);
            indexCommits.add(lastCommit);
        }

        return indexCommits;
    }
}
