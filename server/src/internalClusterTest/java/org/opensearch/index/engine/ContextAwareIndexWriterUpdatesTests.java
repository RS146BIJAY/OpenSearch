/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class ContextAwareIndexWriterUpdatesTests extends EngineTestCase {

    // IW1: 1 D
    public void testDeleteScenario1() throws IOException {
        engine.refresh("warm_up");
        ParsedDocument doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("test"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        engine.index(operation);

        engine.delete(new Engine.Delete(operation.id(), operation.uid(), primaryTerm.get()));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(0, searcher.getIndexReader().numDocs());
        }
    }

    // IW1: 1 2 3 D
    public void testDeleteScenario2() throws IOException {
        engine.refresh("warm_up");
        ParsedDocument doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("test"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        engine.index(operation);

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index update = indexForDoc(doc);
        engine.index(update);

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        update = indexForDoc(doc);
        engine.index(update);

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        update = indexForDoc(doc);
        engine.index(update);

        System.out.println("Deleting term id " + update.id());
        engine.delete(new Engine.Delete(update.id(), update.uid(), primaryTerm.get()));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(0, searcher.getIndexReader().numDocs());
//            assertAtMostOneLuceneDocumentPerSequenceNumber(engine.config().getIndexSettings(), searcher.getDirectoryReader());
        }
    }


    public void testDeleteScenario3() throws IOException {
        engine.refresh("warm_up");
        ParsedDocument doc = testParsedDocument(
                "1",
                null,
                testDocumentWithTextField("test"),
                new BytesArray("{}".getBytes(Charset.defaultCharset())),
                null
        );

        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        Engine.IndexResult result = engine.index(operation);
        System.out.println("Indexing result " + result);

        doc = testParsedDocument(
                "1",
                null,
                testDocumentWithTextField("updated"),
                new BytesArray("{}".getBytes(Charset.defaultCharset())),
                null
        );

        Engine.Index update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);

        doc = testParsedDocument(
                "1",
                null,
                testDocumentWithTextField("updated"),
                new BytesArray("{}".getBytes(Charset.defaultCharset())),
                null
        );

        update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);

        doc = testParsedDocument(
                "1",
                null,
                testDocumentWithTextField("updated"),
                new BytesArray("{}".getBytes(Charset.defaultCharset())),
                null
        );

        update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.getIndexReader().numDocs());
//            assertAtMostOneLuceneDocumentPerSequenceNumber(engine.config().getIndexSettings(), searcher.getDirectoryReader());
        }
    }

    // IW1: 1 2 3 1

    // Pa: 1 (due to version get refresh).
    // 2 3 4 1
    public void testDeleteScenario4() throws IOException {
        engine.refresh("warm_up");
        ParsedDocument doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("test"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        Engine.IndexResult result = engine.index(operation);
        System.out.println("Indexing result " + result);

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);

        System.out.println("Deleting term id " + update.id());
        Engine.DeleteResult deleteResult = engine.delete(new Engine.Delete(update.id(), update.uid(), primaryTerm.get()));
        System.out.println("Delete result " + deleteResult);
        update = indexForDoc(doc);
        result = engine.index(update);
        System.out.println("Indexing result " + result);
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.getIndexReader().numDocs());
            assertAtMostOneLuceneDocumentPerSequenceNumber(engine.config().getIndexSettings(), searcher.getDirectoryReader());
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
        }
    }


    // IW1: 1
    // IW2: D
    public void testDeleteScenario5() throws IOException {

    }

    // 1
    // 2 3 4
    // 5 6 7
    // D 1 (Do not think this will go back as active IndexWriter is always forward. Mark for refresh will never become active).
    public void testDeleteScenario6() throws IOException {

    }

    //
    public void testDeleteScenario20() throws IOException {
        engine.refresh("warm_up");
        ParsedDocument doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("test"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        engine.index(operation);
//        engine.refresh("test");
//        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
//            assertEquals(1, searcher.getIndexReader().numDocs());
//        }

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index update = indexForDoc(doc);
        engine.index(update);
//        engine.refresh("test");
//        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
//            assertEquals(1, searcher.getIndexReader().numDocs());
//        }

        doc = testParsedDocument(
            "1",
            null,
            testDocumentWithTextField("updated"),
            new BytesArray("{}".getBytes(Charset.defaultCharset())),
            null
        );

        Engine.Index update2 = indexForDoc(doc);
        engine.index(update2);
//        engine.refresh("test");
//        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
//            assertEquals(1, searcher.getIndexReader().numDocs());
//        }

        engine.delete(new Engine.Delete(update2.id(), update2.uid(), primaryTerm.get()));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(0, searcher.getIndexReader().numDocs());
        }

        engine.index(update2);
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.getIndexReader().numDocs());
        }
    }



    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        return appendOnlyPrimary(doc, retry, autoGeneratedIdTimestamp, true);
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp, boolean create) {
        return new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            1,
            create ? Versions.MATCH_DELETED : Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            retry,
            UNASSIGNED_SEQ_NO,
            0
        );
    }

}
