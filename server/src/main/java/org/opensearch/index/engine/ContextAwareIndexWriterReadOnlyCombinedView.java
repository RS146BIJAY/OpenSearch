/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.CriteriaBasedCompositeDirectory;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.index.OpenSearchMultiReader;
import org.opensearch.core.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ContextAwareIndexWriterReadOnlyCombinedView
 *
 * @opensearch.api
 */
public class ContextAwareIndexWriterReadOnlyCombinedView implements Closeable {

    private final Map<String, OpenSearchConcurrentMergeScheduler> mergeSchedulerCriteriaMap;
    private final Map<String, CombinedDeletionPolicy> childLevelCombinedDeletionPolicyMap;
    private final Map<String, IndexWriter> criteriaBasedIndexWriters;
    private final Set<String> criteriaList;
    private final ShardId shardId;
    private LiveIndexWriterConfig indexWriterConfig = null;
    private final CriteriaBasedCompositeDirectory directory;
    private Iterable<Map.Entry<String, String>> commitUserData;
    private final AtomicLong changeCount = new AtomicLong();
    private SegmentInfos lastCommitedSegmentInfos;

    // Generation always start from 1. Irrespective of whatever generation child IndexWriter has. This is because we do not
    // persists parent level generations.
    private final Map<Long, String> childLevelGenerationListMap = new HashMap<>();

    public ContextAwareIndexWriterReadOnlyCombinedView(
        final Map<String, OpenSearchConcurrentMergeScheduler> mergeSchedulerCriteriaMap,
        final Map<String, CombinedDeletionPolicy> childLevelCombinedDeletionPolicyMap,
        final Map<String, IndexWriter> criteriaBasedIndexWriters,
        final Directory directory,
        final ShardId shardId
    ) throws IOException {
        this.mergeSchedulerCriteriaMap = mergeSchedulerCriteriaMap;
        this.childLevelCombinedDeletionPolicyMap = childLevelCombinedDeletionPolicyMap;
        this.criteriaBasedIndexWriters = criteriaBasedIndexWriters;
        this.directory = CriteriaBasedCompositeDirectory.unwrap(directory);
        this.criteriaList = this.directory.getCriteriaList();
        this.shardId = shardId;
        this.lastCommitedSegmentInfos = Lucene.readSegmentInfos(directory);
        this.commitUserData = this.lastCommitedSegmentInfos.getUserData().entrySet();
        populateChildLevelGenerationMap();
        for (IndexWriter writer : criteriaBasedIndexWriters.values()) {
            this.indexWriterConfig = writer.getConfig();
            break;
        }
    }

    // For read only engine.
    public ContextAwareIndexWriterReadOnlyCombinedView(final Directory directory, ShardId shardId) throws IOException {
        this.mergeSchedulerCriteriaMap = null;
        this.childLevelCombinedDeletionPolicyMap = null;
        this.criteriaBasedIndexWriters = null;
        this.indexWriterConfig = null;
        this.directory = CriteriaBasedCompositeDirectory.unwrap(directory);
        this.lastCommitedSegmentInfos = Lucene.readSegmentInfos(directory);
        this.commitUserData = this.lastCommitedSegmentInfos.getUserData().entrySet();
        this.criteriaList = this.directory.getCriteriaList();
        this.shardId = shardId;
        populateChildLevelGenerationMap();
    }

    // This should be synchronised??
    private void populateChildLevelGenerationMap() throws IOException {
        final long generation = lastCommitedSegmentInfos.getGeneration();
        final StringBuilder generationString = new StringBuilder();
        for (Directory directory : directory.getChildDirectoryList()) {
            generationString.append(Lucene.readSegmentInfos(directory).getGeneration()).append(",");
        }

        childLevelGenerationListMap.put(generation, generationString.toString());
    }

    public Iterable<Map.Entry<String, String>> getUserData() {
        return commitUserData;
    }

    public OpenSearchMultiReader openMultiReader() throws IOException {
        final Map<String, DirectoryReader> readerCriteriaMap = new HashMap<>();
        for (String criteria : criteriaList) {
            final OpenSearchDirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(
                DirectoryReader.open(criteriaBasedIndexWriters.get(criteria)),
                shardId
            );

            readerCriteriaMap.put(criteria, directoryReader);
        }

        return new OpenSearchMultiReader(directory, readerCriteriaMap, shardId);
    }

    public SegmentInfos getLatestSegmentInfos(boolean isExtendedCompatibility, Version minimumSupportedVersion) throws IOException {
        if (isExtendedCompatibility) {
            return Lucene.readSegmentInfos(directory, minimumSupportedVersion);
        } else {
            return Lucene.readSegmentInfos(directory);
        }
    }

    public Map<String, Long> getChildLastGenerationList() {
        Map<String, Long> lastGenerationList = new HashMap<>();
        String[] generationList = childLevelGenerationListMap.get(lastCommitedSegmentInfos.getGeneration()).split(",");
        int i = 0;
        for (String criteria : criteriaList) {
            lastGenerationList.put(criteria, Long.parseLong(generationList[i++]));
        }

        return lastGenerationList;
    }

    public void rollback() throws IOException {
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            indexWriter.rollback();
        }

        // Fix this.
    }

    @Override
    public void close() throws IOException {
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            indexWriter.close();
        }
    }

    public boolean hasSnapshottedCommits() {
        for (CombinedDeletionPolicy combinedDeletionPolicy : childLevelCombinedDeletionPolicyMap.values()) {
            if (combinedDeletionPolicy.hasSnapshottedCommits()) {
                return true;
            }
        }

        return false;
    }

    public long getFlushingBytes() {
        long flushingBytes = 0;
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            flushingBytes += indexWriter.getFlushingBytes();
        }

        return flushingBytes;
    }

    public Throwable getTragicException() {
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            if (indexWriter.getTragicException() != null) {
                return indexWriter.getTragicException();
            }
        }

        return null;
    }

    public long getPendingNumDocs() {
        long pendingNumDocsCount = 0;
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            pendingNumDocsCount += indexWriter.getPendingNumDocs();
        }

        return pendingNumDocsCount;
    }

    public LiveIndexWriterConfig getConfig() {
        return indexWriterConfig;
    }

    public long ramBytesUsed() {
        long ramBytesUsed = 0;
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            ramBytesUsed += indexWriter.ramBytesUsed();
        }

        return ramBytesUsed;
    }

    /**
     * Sets the commit user data iterator, controlling whether to advance the {@link
     * SegmentInfos#getVersion}.
     *
     * @lucene.internal
     */
    public final synchronized void setLiveCommitData(
        Iterable<Map.Entry<String, String>> commitUserData, boolean doIncrementVersion) {
        this.commitUserData = commitUserData;
        // TODO: Is this required?
//        if (doIncrementVersion) {
//            segmentInfos.changed();
//        }

        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            indexWriter.setLiveCommitData(commitUserData, doIncrementVersion);
        }

        changeCount.incrementAndGet();
    }

    public boolean hasUncommittedChanges() {
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            if (indexWriter.hasUncommittedChanges()) {
                return true;
            }
        }

        return false;
    }

    public void commit() throws IOException {
        for (IndexWriter indexWriter : criteriaBasedIndexWriters.values()) {
            if (indexWriter.hasUncommittedChanges()) {
                indexWriter.commit();
            }
        }

        SegmentInfos combinedSegmentInfos = Lucene.readSegmentInfos(directory);
        combinedSegmentInfos.updateGeneration(lastCommitedSegmentInfos);
        combinedSegmentInfos.version = lastCommitedSegmentInfos.version + 1;
        combinedSegmentInfos.commit(directory);
        lastCommitedSegmentInfos = combinedSegmentInfos;
    }

    // public IndexCommit acquireIndexCommit() {
    //
    // }

    private List<IndexCommit> getLastIndexCommits(boolean acquireSafeCommit) throws IOException {
        final List<IndexCommit> indexCommits = new ArrayList<>();
        for (Map.Entry<String, CombinedDeletionPolicy> entry : childLevelCombinedDeletionPolicyMap.entrySet()) {
            CombinedDeletionPolicy combinedDeletionPolicy = entry.getValue();
            final IndexCommit lastCommit = combinedDeletionPolicy.acquireIndexCommit(acquireSafeCommit);
            indexCommits.add(lastCommit);
        }

        return indexCommits;
    }
}
