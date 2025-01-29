/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@PublicApi(since = "2.19.0")
public class OpenSearchMultiReader extends MultiReader {
    private final List<IndexReader> subReadersList;
    private final Directory directory;
    private final Set<String> criteriaList;
    private final ShardId shardId;

    public OpenSearchMultiReader(Directory directory, List<StandardDirectoryReader> subReadersList, Set<String> criteriaList, ShardId shardId)  throws IOException {
        super(subReadersList.toArray(IndexReader[]::new));
        this.subReadersList = new ArrayList<>(subReadersList);
        this.directory = directory;
        this.criteriaList = criteriaList;
        this.shardId = shardId;
    }

    public OpenSearchMultiReader(List<IndexReader> subReadersList, Directory directory, Set<String> criteriaList, ShardId shardId) throws IOException {
        super(subReadersList.toArray(IndexReader[]::new));
        this.subReadersList = subReadersList;
        this.directory = directory;
        this.criteriaList = criteriaList;
        this.shardId = shardId;
    }

    public OpenSearchMultiReader(Directory directory, Set<String> criteriaList, ShardId shardId, IndexReader... subReaders) throws IOException {
        super(subReaders);
        this.subReadersList = Collections.unmodifiableList(Arrays.asList(subReaders));
        this.directory = directory;
        this.criteriaList = criteriaList;
        this.shardId = shardId;
    }

    public OpenSearchMultiReader(IndexReader[] subReaders, boolean closeSubReaders, Directory directory, Set<String> criteriaList, ShardId shardId) throws IOException {
        super(subReaders, closeSubReaders);
        this.subReadersList = Collections.unmodifiableList(Arrays.asList(subReaders));
        this.directory = directory;
        this.criteriaList = criteriaList;
        this.shardId = shardId;
    }

    public OpenSearchMultiReader(IndexReader[] subReaders, Comparator<IndexReader> subReadersSorter, boolean closeSubReaders, Directory directory, Set<String> criteriaList, ShardId shardId) throws IOException {
        super(subReaders, subReadersSorter, closeSubReaders);
        this.subReadersList = Collections.unmodifiableList(Arrays.asList(subReaders));
        this.directory = directory;
        this.criteriaList = criteriaList;
        this.shardId = shardId;
    }

    public List<OpenSearchDirectoryReader> getSubDirectoryReaderList() {
        final List<OpenSearchDirectoryReader> sequentialReader = new ArrayList<>();
        for (IndexReader indexReader: subReadersList) {
            assert indexReader instanceof OpenSearchDirectoryReader;
            sequentialReader.add((OpenSearchDirectoryReader) indexReader);
        }

        return sequentialReader;
    }

    public SegmentInfos getSegmentInfos() throws IOException {
        final List<SegmentInfos> segmentInfos = new ArrayList<>();
        for (IndexReader indexReader: subReadersList) {
            assert indexReader instanceof StandardDirectoryReader;
            segmentInfos.add(((StandardDirectoryReader) indexReader).getSegmentInfos());
        }

        return Lucene.combineSegmentInfos(segmentInfos, criteriaList, directory);
    }

    public Directory getDirectory() {
        return directory;
    }

    public Set<String> getCriteriaList() {
        return criteriaList;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public static void addReaderCloseListener(MultiReader reader, IndexReader.ClosedListener listener) {
        OpenSearchMultiReader openSearchMultiReader = getOpenSearchDirectoryReader(reader);
        if (openSearchMultiReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an OpenSearchDirectoryReader/OpenSearchLeafReader"
            );
        }
        IndexReader.CacheHelper cacheHelper = openSearchMultiReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + openSearchMultiReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    public static OpenSearchMultiReader getOpenSearchDirectoryReader(MultiReader reader) {
        if (reader instanceof OpenSearchMultiReader) {
            return (OpenSearchMultiReader) reader;
        }

        return null;
    }
}
