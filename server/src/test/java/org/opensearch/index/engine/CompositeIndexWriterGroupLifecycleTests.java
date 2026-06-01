/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CriteriaBasedCodec;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.test.IndexSettingsModule;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Set;

public class CompositeIndexWriterGroupLifecycleTests extends CriteriaBasedCompositeIndexWriterBaseTests {

    private static final String TENANT_A = "tenantA";
    private static final String TENANT_B = "tenantB";
    private static final String TENANT_C = "tenantC";

    @BeforeClass
    public static void enableFeatureFlag() {
        FeatureFlags.initializeFeatureFlags(
            Settings.builder().put(FeatureFlags.CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG, true).build()
        );
    }

    private IndexSettings casIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder().put("index.context_aware.enabled", true).build()
        );
    }

    @Override
    public EngineConfig config() {
        return config(
            casIndexSettings(),
            store,
            primaryTranslogDir,
            NoMergePolicy.INSTANCE,
            null,
            null,
            null,
            null,
            null,
            new NoneCircuitBreakerService(),
            null
        );
    }

    public void testDeleteGroupExcludesFromFilteredReader() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1", "a2", "a3");
        indexDocsForGroup(compositeWriter, TENANT_B, "b1", "b2");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        // Before deletion: both groups visible
        try (DirectoryReader reader = DirectoryReader.open(parentWriter)) {
            assertEquals(5, reader.numDocs());
            // Verify bucket attributes are present
            for (LeafReaderContext ctx : reader.leaves()) {
                if (ctx.reader() instanceof SegmentReader) {
                    SegmentCommitInfo info = ((SegmentReader) ctx.reader()).getSegmentInfo();
                    String bucket = info.info.getAttribute(CriteriaBasedCodec.BUCKET_NAME);
                    assertNotNull("BUCKET_NAME attribute must be set on segment", bucket);
                }
            }
            assertEquals(3, countDocsForGroup(reader, TENANT_A));
            assertEquals(2, countDocsForGroup(reader, TENANT_B));
        }

        // Delete tenant A
        long invisibleDocs = compositeWriter.deleteGroup(TENANT_A);
        assertEquals(3, invisibleDocs);

        GroupReaderManager groupManager = compositeWriter.getGroupReaderManager();
        assertTrue(groupManager.isGroupDeleted(TENANT_A));
        assertFalse(groupManager.isGroupDeleted(TENANT_B));

        // Filtered reader excludes tenant A
        try (DirectoryReader rawReader = DirectoryReader.open(parentWriter)) {
            DirectoryReader filtered = groupManager.wrapReader(rawReader);
            IndexSearcher searcher = new IndexSearcher(filtered);
            TopDocs results = searcher.search(new MatchAllDocsQuery(), 100);
            assertEquals(2, results.totalHits.value());
        }

        compositeWriter.close();
    }

    public void testDeleteGroupDiscardsNewWritesOnRefresh() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1", "a2");
        indexDocsForGroup(compositeWriter, TENANT_B, "b1");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        // Delete tenant A
        compositeWriter.deleteGroup(TENANT_A);

        // New writes to deleted group
        indexDocsForGroup(compositeWriter, TENANT_A, "a3", "a4");
        indexDocsForGroup(compositeWriter, TENANT_B, "b2");

        // Refresh - tenant A new writes should be discarded
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        try (DirectoryReader reader = DirectoryReader.open(parentWriter)) {
            // Original a1, a2 + b1 from first refresh = 3
            // Second refresh: a3, a4 discarded, b2 synced = +1
            assertEquals(4, reader.numDocs());
            assertEquals(2, countDocsForGroup(reader, TENANT_A)); // still has a1, a2 from before delete
            assertEquals(2, countDocsForGroup(reader, TENANT_B)); // b1 + b2
        }

        compositeWriter.close();
    }

    public void testFreezeGroupStopsRefreshButRemainsSearchable() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1", "a2");
        indexDocsForGroup(compositeWriter, TENANT_B, "b1");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        compositeWriter.freezeGroup(TENANT_A);
        assertTrue(compositeWriter.getGroupReaderManager().isGroupFrozen(TENANT_A));

        // Existing data still searchable
        try (DirectoryReader reader = DirectoryReader.open(parentWriter)) {
            assertEquals(2, countDocsForGroup(reader, TENANT_A));
        }

        // New writes to frozen group discarded on refresh
        indexDocsForGroup(compositeWriter, TENANT_A, "a3");
        indexDocsForGroup(compositeWriter, TENANT_B, "b2");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        try (DirectoryReader reader = DirectoryReader.open(parentWriter)) {
            assertEquals(2, countDocsForGroup(reader, TENANT_A)); // a3 discarded
            assertEquals(2, countDocsForGroup(reader, TENANT_B)); // b2 synced
        }

        compositeWriter.close();
    }

    public void testUnfreezeGroupResumesOperations() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        compositeWriter.freezeGroup(TENANT_A);
        compositeWriter.unfreezeGroup(TENANT_A);
        assertFalse(compositeWriter.getGroupReaderManager().isGroupFrozen(TENANT_A));

        indexDocsForGroup(compositeWriter, TENANT_A, "a2", "a3");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        try (DirectoryReader reader = DirectoryReader.open(parentWriter)) {
            assertEquals(3, countDocsForGroup(reader, TENANT_A));
        }

        compositeWriter.close();
    }

    public void testDeleteMultipleGroupsIndependently() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1", "a2");
        indexDocsForGroup(compositeWriter, TENANT_B, "b1", "b2", "b3");
        indexDocsForGroup(compositeWriter, TENANT_C, "c1");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        compositeWriter.deleteGroup(TENANT_A);
        compositeWriter.deleteGroup(TENANT_C);

        GroupReaderManager groupManager = compositeWriter.getGroupReaderManager();
        assertTrue(groupManager.isGroupDeleted(TENANT_A));
        assertFalse(groupManager.isGroupDeleted(TENANT_B));
        assertTrue(groupManager.isGroupDeleted(TENANT_C));

        try (DirectoryReader rawReader = DirectoryReader.open(parentWriter)) {
            DirectoryReader filtered = groupManager.wrapReader(rawReader);
            IndexSearcher searcher = new IndexSearcher(filtered);
            assertEquals(3, searcher.search(new MatchAllDocsQuery(), 100).totalHits.value());
        }

        compositeWriter.close();
    }

    public void testGroupScopedReaderOnlyIncludesTargetGroup() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1", "a2", "a3");
        indexDocsForGroup(compositeWriter, TENANT_B, "b1", "b2");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        GroupReaderManager groupManager = compositeWriter.getGroupReaderManager();

        try (DirectoryReader rawReader = DirectoryReader.open(parentWriter)) {
            DirectoryReader scopedA = groupManager.getReaderForGroup(rawReader, TENANT_A);
            assertNotNull(scopedA);
            assertEquals(3, new IndexSearcher(scopedA).search(new MatchAllDocsQuery(), 100).totalHits.value());

            DirectoryReader scopedB = groupManager.getReaderForGroup(rawReader, TENANT_B);
            assertNotNull(scopedB);
            assertEquals(2, new IndexSearcher(scopedB).search(new MatchAllDocsQuery(), 100).totalHits.value());

            // Deleted group returns null
            groupManager.deleteGroup(TENANT_A);
            assertNull(groupManager.getReaderForGroup(rawReader, TENANT_A));
        }

        compositeWriter.close();
    }

    public void testPerGroupDirtyTracking() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        GroupReaderManager groupManager = compositeWriter.getGroupReaderManager();
        long gen0 = groupManager.getCurrentGeneration();

        // Only tenant A writes in this cycle
        indexDocsForGroup(compositeWriter, TENANT_A, "a1");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);
        groupManager.onRefresh();

        assertTrue(groupManager.isGroupModifiedSince(TENANT_A, gen0));
        assertFalse(groupManager.isGroupModifiedSince(TENANT_B, gen0));

        long gen1 = groupManager.getCurrentGeneration();

        // Only tenant B writes in this cycle
        indexDocsForGroup(compositeWriter, TENANT_B, "b1");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);
        groupManager.onRefresh();

        assertFalse(groupManager.isGroupModifiedSince(TENANT_A, gen1));
        assertTrue(groupManager.isGroupModifiedSince(TENANT_B, gen1));

        compositeWriter.close();
    }

    public void testGetGroupsToSkipOnRefresh() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        GroupReaderManager groupManager = compositeWriter.getGroupReaderManager();
        assertTrue(groupManager.getGroupsToSkipOnRefresh().isEmpty());

        compositeWriter.deleteGroup(TENANT_A);
        compositeWriter.freezeGroup(TENANT_B);

        Set<String> toSkip = groupManager.getGroupsToSkipOnRefresh();
        assertTrue(toSkip.contains(TENANT_A));
        assertTrue(toSkip.contains(TENANT_B));
        assertFalse(toSkip.contains(TENANT_C));

        compositeWriter.close();
    }

    public void testHasStateChangedFlag() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        GroupReaderManager groupManager = compositeWriter.getGroupReaderManager();
        assertFalse(groupManager.hasStateChanged());

        compositeWriter.deleteGroup(TENANT_A);
        assertTrue(groupManager.hasStateChanged());
        assertFalse(groupManager.hasStateChanged()); // reset on read

        compositeWriter.freezeGroup(TENANT_B);
        assertTrue(groupManager.hasStateChanged());
        assertFalse(groupManager.hasStateChanged());

        compositeWriter.close();
    }

    public void testEmptyLeafReaderReportsZeroDocs() throws IOException {
        IndexWriter parentWriter = createWriter();
        CompositeIndexWriter compositeWriter = new CompositeIndexWriter(
            config(),
            parentWriter,
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        indexDocsForGroup(compositeWriter, TENANT_A, "a1", "a2");
        compositeWriter.beforeRefresh();
        compositeWriter.afterRefresh(true);

        try (DirectoryReader reader = DirectoryReader.open(parentWriter)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                GroupReaderManager.EmptyLeafReader empty = new GroupReaderManager.EmptyLeafReader(ctx.reader());
                assertEquals(0, empty.numDocs());
                // maxDoc() preserved from underlying reader (needed for Lucene internal consistency)
                assertTrue(empty.maxDoc() > 0);
            }
        }

        compositeWriter.close();
    }

    // ─── Helpers ───

    private void indexDocsForGroup(CompositeIndexWriter writer, String group, String... ids) throws IOException {
        for (String id : ids) {
            ParsedDocument doc = createParsedDoc(id, null, group);
            writer.addDocument(doc.docs().get(0), newUid(doc));
        }
    }

    private long countDocsForGroup(DirectoryReader reader, String group) {
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
}