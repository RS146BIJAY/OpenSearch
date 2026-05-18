/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.DeleterImpl;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Lucene-based implementation of {@link DeleteExecutionEngine} that tracks per-generation
 * deleters paired with their corresponding writers. Each deleter delegates document
 * deletion to the underlying {@link LuceneWriter}.
 *
 * @opensearch.experimental
 */
public class LuceneDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private static final Logger logger = LogManager.getLogger(LuceneDeleteExecutionEngine.class);

    private final Map<Long, Deleter> generationToDeleterMap;
    private final DataFormat dataFormat;
    private final IndexWriter parentWriter;
    private final ConcurrentMap<BytesRef, Long> idToGen;

    public LuceneDeleteExecutionEngine(DataFormat dataFormat, Committer committer) {
        this.generationToDeleterMap = new ConcurrentHashMap<>();
        this.idToGen = new ConcurrentHashMap<>();
        this.dataFormat = dataFormat;
        this.parentWriter = ((LuceneCommitter) committer).getIndexWriter();
    }

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        LuceneWriter luceneWriter = writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)
            .map(w -> (LuceneWriter) w)
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot create deleter: no Lucene writer found for generation=" + writer.generation())
            );
        Deleter deleter = new DeleterImpl<>(luceneWriter);
        generationToDeleterMap.put(writer.generation(), deleter);
        return deleter;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
        Deleter currentDeleter = generationToDeleterMap.get(deleteInput.generation());
        currentDeleter.recordBufferedDeletes(deleteInput.id());
        Long previousGen = lookupGen(new Term(deleteInput.fieldName(), Uid.encodeId(deleteInput.id())).bytes());
        if (previousGen != null) {
            // It means previous writer is active here.
            // TODO: Check for race condition here.
            Deleter deleter = generationToDeleterMap.get(previousGen);
            return deleter.deleteDoc(deleteInput);
        }

        return new DeleteResult.Success(1L, 1L, 1L);
    }

    @Override
    public DataFormat getDataFormat() {
        return this.dataFormat;
    }

    @Override
    public void close() throws IOException {
        for (Deleter deleter : generationToDeleterMap.values()) {
            deleter.close();
        }

        generationToDeleterMap.clear();
        idToGen.clear();
    }

    private Long lookupGen(BytesRef id) {
        return idToGen.get(id);
    }

    @Override
    public void recordWrite(BytesRef id, long generation) {
        idToGen.put(id, generation);
    }

    @Override
    public boolean purgeGenerationsAndApplyDeleteToParent(List<Long> generations) throws IOException {
        if (generations.isEmpty()) {
            return false;
        }

        final Set<Long> purged = Set.copyOf(generations);
        // 1. Drop idToGen entries pointing at any purged gen.
        for (Map.Entry<BytesRef, Long> e : idToGen.entrySet()) {
            if (purged.contains(e.getValue())) {
                idToGen.remove(e.getKey());
            }
        }

        int totalApplied = 0;
        // 2. Remove deleters and clear their buffered deletes under the deleter lock.
        for (long gen : generations) {
            Deleter deleter = generationToDeleterMap.remove(gen);
            Queue<String> bufferedDeletes = deleter.bufferedDeletes();
            for (String deletedId: bufferedDeletes) {
                parentWriter.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(deletedId)));
                totalApplied++;
            }

            // TODO: Should I close these deleter here??
            bufferedDeletes.clear();
        }

        return totalApplied > 0;
    }
}
