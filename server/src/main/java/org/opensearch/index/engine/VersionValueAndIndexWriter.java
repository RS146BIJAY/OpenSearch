/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;

public class VersionValueAndIndexWriter {

    private final VersionValue versionValue;
    private final IndexWriter indexWriter;

    public VersionValueAndIndexWriter(VersionValue versionValue, IndexWriter indexWriter) {
        this.versionValue = versionValue;
        this.indexWriter = indexWriter;
    }

    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    public VersionValue getVersionValue() {
        return versionValue;
    }
}
