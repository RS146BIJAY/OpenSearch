/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * OpenSearchMultiReader
 *
 * @opensearch.api
 */
@PublicApi(since = "2.19.0")
public class OpenSearchMultiReader extends MultiReader {
    private final Map<String, DirectoryReader> subReadersCriteriaMap;
    private final Directory directory;
    private final ShardId shardId;

    public OpenSearchMultiReader(Directory directory, Map<String, DirectoryReader> subReadersCriteriaMap, ShardId shardId)
        throws IOException {
        super(subReadersCriteriaMap.values().stream().filter(Objects::nonNull).toArray(IndexReader[]::new));
        this.subReadersCriteriaMap = subReadersCriteriaMap;
        this.directory = directory;
        this.shardId = shardId;
    }

    public SegmentInfos getSegmentInfos() throws IOException {
        final Map<String, SegmentInfos> segmentInfosCriteriaMap = new HashMap<>();
        for (Map.Entry<String, DirectoryReader> subReaderEntry : subReadersCriteriaMap.entrySet()) {
            DirectoryReader directoryReader = OpenSearchDirectoryReader.unwrap(subReaderEntry.getValue());
            if (Lucene.indexExists(directoryReader.directory())) {
                String criterion = subReaderEntry.getKey();
                assert directoryReader instanceof StandardDirectoryReader;
                segmentInfosCriteriaMap.put(criterion, ((StandardDirectoryReader) directoryReader).getSegmentInfos());
            }
        }

        return Lucene.combineSegmentInfos(segmentInfosCriteriaMap, directory, false);
    }

    public Directory getDirectory() {
        return directory;
    }

    public Set<String> getCriteriaList() {
        return subReadersCriteriaMap.keySet();
    }

    public Map<String, DirectoryReader> getSubReadersCriteriaMap() {
        return subReadersCriteriaMap;
    }

    public ShardId shardId() {
        return shardId;
    }

    public static void addReaderCloseListener(OpenSearchMultiReader reader, IndexReader.ClosedListener listener, List<String> criteriaList)
        throws IOException {
        OpenSearchMultiReader openSearchMultiReader = getOpenSearchMultiDirectoryReader(reader);
        if (openSearchMultiReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an OpenSearchDirectoryReader/OpenSearchLeafReader"
            );
        }

        for (String criteria : criteriaList) {
            IndexReader.CacheHelper cacheHelper = openSearchMultiReader.getReaderCacheHelper(criteria);
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + openSearchMultiReader + " does not support caching");
            }

            // TODO: Do we need this?
            // assert cacheHelper.getKey() == reader.getReaderCacheHelper(criteria).getKey();
            cacheHelper.addClosedListener(listener);
        }
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        throw new UnsupportedOperationException();
    }

    public CacheHelper getReaderCacheHelper(String criteria) {
        return subReadersCriteriaMap.get(criteria).getReaderCacheHelper();
    }

    public static OpenSearchMultiReader getOpenSearchMultiDirectoryReader(MultiReader reader) throws IOException {
        if (reader instanceof OpenSearchMultiReader) {
            Map<String, DirectoryReader> subReadersMap = new HashMap<>();
            OpenSearchMultiReader openSearchMultiReader = (OpenSearchMultiReader) reader;
            for (Map.Entry<String, DirectoryReader> subReaderEntry : openSearchMultiReader.getSubReadersCriteriaMap().entrySet()) {
                DirectoryReader subReader = subReaderEntry.getValue();
                OpenSearchDirectoryReader openSearchDirectoryReader = OpenSearchDirectoryReader.getOpenSearchDirectoryReader(subReader);
                if (openSearchDirectoryReader != null) {
                    subReadersMap.put(subReaderEntry.getKey(), openSearchDirectoryReader);
                }
            }

            if (subReadersMap.isEmpty()) {
                return null;
            } else {
                return new OpenSearchMultiReader(openSearchMultiReader.getDirectory(), subReadersMap, openSearchMultiReader.shardId);
            }
        }

        return null;
    }

    public boolean isCurrent() throws IOException {
        for (IndexReader indexReader : subReadersCriteriaMap.values()) {
            assert indexReader instanceof OpenSearchDirectoryReader;
            if (!((OpenSearchDirectoryReader) indexReader).isCurrent()) {
                return false;
            }
        }

        return true;
    }

    public static OpenSearchMultiReader unwrap(OpenSearchMultiReader reader) throws IOException {
        Map<String, DirectoryReader> localSubReaderMap = new HashMap<>();
        Map<String, DirectoryReader> subReadersMap = reader.getSubReadersCriteriaMap();
        for (String criteria : reader.getCriteriaList()) {
            DirectoryReader directoryReader = OpenSearchDirectoryReader.unwrap(subReadersMap.get(criteria));
            if (directoryReader != null) {
                localSubReaderMap.put(criteria, directoryReader);
            }
        }

        if (!subReadersMap.isEmpty()) {
            return new OpenSearchMultiReader(reader.getDirectory(), localSubReaderMap, reader.shardId);
        }

        return null;
    }

    public static OpenSearchMultiReader open(
        Directory directory,
        ShardId shardId,
        Function<DirectoryReader, DirectoryReader> readerWrapperFunction
    ) throws IOException {
        CriteriaBasedCompositeDirectory compositeDirectory = CriteriaBasedCompositeDirectory.unwrap(directory);
        Map<String, DirectoryReader> subReaderCriteriaMap = new HashMap<>();
        if (compositeDirectory != null) {
            for (Map.Entry<String, Directory> childDirectoryEntry : compositeDirectory.getCriteriaDirectoryMapping().entrySet()) {
                subReaderCriteriaMap.put(
                    childDirectoryEntry.getKey(),
                    readerWrapperFunction.apply(DirectoryReader.open(childDirectoryEntry.getValue()))
                );
            }
        }

        return new OpenSearchMultiReader(directory, subReaderCriteriaMap, shardId);
    }

    public static OpenSearchMultiReader open(
        IndexCommit commit,
        int minSupportedMajorVersion,
        ShardId shardId,
        Function<DirectoryReader, DirectoryReader> readerWrapperFunction,
        Comparator<LeafReader> leafSorter
    ) throws IOException {
        final Map<String, DirectoryReader> childReaderMap = new HashMap<>();
        final Map<String, IndexCommit> subReaderCommit = Lucene.listSubCommits(commit);
        for (Map.Entry<String, IndexCommit> subReaderEntry : subReaderCommit.entrySet()) {
            String criteria = subReaderEntry.getKey();
            DirectoryReader directoryReader = DirectoryReader.open(subReaderEntry.getValue(), minSupportedMajorVersion, leafSorter);
            childReaderMap.put(criteria, readerWrapperFunction.apply(directoryReader));
        }

        return new OpenSearchMultiReader(commit.getDirectory(), childReaderMap, shardId);
    }

    public static OpenSearchMultiReader open(
        IndexCommit commit,
        ShardId shardId,
        Function<DirectoryReader, DirectoryReader> readerWrapperFunction
    ) throws IOException {
        final Map<String, DirectoryReader> childReaderMap = new HashMap<>();
        final Map<String, IndexCommit> subReaderCommit = Lucene.listSubCommits(commit);
        for (Map.Entry<String, IndexCommit> subReaderEntry : subReaderCommit.entrySet()) {
            String criteria = subReaderEntry.getKey();
            DirectoryReader directoryReader = DirectoryReader.open(subReaderEntry.getValue());
            childReaderMap.put(criteria, readerWrapperFunction.apply(directoryReader));
        }

        return new OpenSearchMultiReader(commit.getDirectory(), childReaderMap, shardId);
    }

    @Override
    protected synchronized void doClose() throws IOException {
        IOException ioe = null;
        for (final IndexReader r : getSequentialSubReaders()) {
            r.decRef();
            try {
                if (r.getRefCount() == 0) {
                    r.close();
                }
            } catch (IOException e) {
                if (ioe == null) ioe = e;
            }
        }
        // throw the first exception
        if (ioe != null) throw ioe;
    }

}
