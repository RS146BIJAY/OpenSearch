/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.index.codec.CriteriaBasedCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-group (sub-shard) lifecycle for Context-Aware Segments.
 *
 * <p>After addIndexes() combines child writers into the accumulating writer, each segment
 * retains its {@code BUCKET_NAME} attribute. This class tracks which groups are deleted or
 * frozen, and provides filtered readers that exclude those groups.</p>
 */
public class GroupReaderManager {

    private final Set<String> deletedGroups = ConcurrentHashMap.newKeySet();
    private final Set<String> frozenGroups = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> groupRefreshGeneration = new ConcurrentHashMap<>();
    private volatile long currentGeneration = 0;
    private volatile boolean stateChanged = false;

    public void deleteGroup(String group) {
        deletedGroups.add(group);
        stateChanged = true;
    }

    public void freezeGroup(String group) {
        frozenGroups.add(group);
        stateChanged = true;
    }

    public void unfreezeGroup(String group) {
        frozenGroups.remove(group);
        stateChanged = true;
    }

    public boolean isGroupDeleted(String group) {
        return deletedGroups.contains(group);
    }

    public boolean isGroupFrozen(String group) {
        return frozenGroups.contains(group);
    }

    public Set<String> getDeletedGroups() {
        return Set.copyOf(deletedGroups);
    }

    public Set<String> getGroupsToSkipOnRefresh() {
        Set<String> skip = ConcurrentHashMap.newKeySet();
        skip.addAll(deletedGroups);
        skip.addAll(frozenGroups);
        return skip;
    }

    public void markGroupDirty(String group) {
        groupRefreshGeneration.put(group, currentGeneration);
    }

    public boolean isGroupModifiedSince(String group, long sinceGeneration) {
        Long lastModified = groupRefreshGeneration.get(group);
        return lastModified != null && lastModified >= sinceGeneration;
    }

    public void onRefresh() {
        currentGeneration++;
    }

    public long getCurrentGeneration() {
        return currentGeneration;
    }

    public boolean hasStateChanged() {
        boolean changed = stateChanged;
        stateChanged = false;
        return changed;
    }

    /**
     * Wraps a DirectoryReader to exclude segments belonging to deleted groups.
     * Uses FilterDirectoryReader with a SubReaderWrapper that makes excluded segments
     * report numDocs=0. Lucene's search infrastructure skips segments with numDocs=0.
     */
    public DirectoryReader wrapReader(DirectoryReader reader) throws IOException {
        if (deletedGroups.isEmpty()) {
            return reader;
        }
        return new GroupFilteredDirectoryReader(reader, deletedGroups);
    }

    /**
     * Returns a reader scoped to a single group. Returns null if the group is deleted.
     */
    public DirectoryReader getReaderForGroup(DirectoryReader reader, String group) throws IOException {
        if (deletedGroups.contains(group)) {
            return null;
        }
        return new GroupScopedDirectoryReader(reader, group);
    }

    /**
     * Count live docs belonging to a group across all leaves.
     */
    public long countDocsForGroup(DirectoryReader reader, String group) {
        long count = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            if (ctx.reader() instanceof SegmentReader) {
                SegmentCommitInfo info = ((SegmentReader) ctx.reader()).getSegmentInfo();
                String bucket = info.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME);
                if (group.equals(bucket)) {
                    count += ctx.reader().numDocs();
                }
            }
        }
        return count;
    }

    // ─── FilterDirectoryReader that EXCLUDES deleted groups ───

    static class GroupFilteredDirectoryReader extends FilterDirectoryReader {

        private final Set<String> excludedGroups;

        GroupFilteredDirectoryReader(DirectoryReader in, Set<String> excludedGroups) throws IOException {
            super(in, new GroupExclusionWrapper(excludedGroups));
            this.excludedGroups = excludedGroups;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new GroupFilteredDirectoryReader(in, excludedGroups);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    // ─── FilterDirectoryReader that INCLUDES only one group ───

    static class GroupScopedDirectoryReader extends FilterDirectoryReader {

        private final String targetGroup;

        GroupScopedDirectoryReader(DirectoryReader in, String targetGroup) throws IOException {
            super(in, new GroupInclusionWrapper(targetGroup));
            this.targetGroup = targetGroup;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new GroupScopedDirectoryReader(in, targetGroup);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    // ─── SubReaderWrappers ───

    static class GroupExclusionWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final Set<String> excludedGroups;

        GroupExclusionWrapper(Set<String> excludedGroups) {
            this.excludedGroups = excludedGroups;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            if (reader instanceof SegmentReader) {
                SegmentCommitInfo info = ((SegmentReader) reader).getSegmentInfo();
                String bucket = info.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME);
                if (bucket != null && excludedGroups.contains(bucket)) {
                    return new EmptyLeafReader(reader);
                }
            }
            return reader;
        }
    }

    static class GroupInclusionWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final String targetGroup;

        GroupInclusionWrapper(String targetGroup) {
            this.targetGroup = targetGroup;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            if (reader instanceof SegmentReader) {
                SegmentCommitInfo info = ((SegmentReader) reader).getSegmentInfo();
                String bucket = info.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME);
                if (!targetGroup.equals(bucket)) {
                    return new EmptyLeafReader(reader);
                }
            }
            return reader;
        }
    }

    // ─── LeafReader that reports 0 docs (makes a segment invisible) ───

    static class EmptyLeafReader extends FilterLeafReader {

        EmptyLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public int numDocs() {
            return 0;
        }

        @Override
        public org.apache.lucene.util.Bits getLiveDocs() {
            // Return a Bits that marks ALL docs as deleted
            final int max = in.maxDoc();
            return new org.apache.lucene.util.Bits() {
                @Override
                public boolean get(int index) {
                    return false; // false = deleted
                }

                @Override
                public int length() {
                    return max;
                }
            };
        }

        @Override
        public boolean hasDeletions() {
            return true;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}