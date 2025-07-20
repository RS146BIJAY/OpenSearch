/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CompositeIndexWriter {

    private Map<String, IndexWriter> activeChildIndexWriterMap = new ConcurrentHashMap<>();
    private final Map<String, Set<IndexWriter>> markForRefreshChildIndexWriterMap = new ConcurrentHashMap<>();

    public CompositeIndexWriter(IndexWriterConfig config) {

    }

}
