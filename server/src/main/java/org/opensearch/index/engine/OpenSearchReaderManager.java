/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.index.OpenSearchMultiReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to safely share {@link OpenSearchDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @see SearcherManager
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "reference counting is required here")
class OpenSearchReaderManager extends ReferenceManager<OpenSearchMultiReader> {
    /**
     * Creates and returns a new OpenSearchReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     */
    OpenSearchReaderManager(OpenSearchMultiReader reader) {
        this.current = reader;
    }

    @Override
    protected void decRef(OpenSearchMultiReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected OpenSearchMultiReader refreshIfNeeded(OpenSearchMultiReader referenceToRefresh) throws IOException {
        final Map<String, DirectoryReader> originalReadersMap = referenceToRefresh.getSubReadersCriteriaMap();
        final Map<String, DirectoryReader> newReadersMap = new HashMap<>();
        for (Map.Entry<String, DirectoryReader> readerCriteriaEntry : originalReadersMap.entrySet()) {
            DirectoryReader refreshedReader = DirectoryReader.openIfChanged(readerCriteriaEntry.getValue());
            if (refreshedReader != null) {
                newReadersMap.put(readerCriteriaEntry.getKey(), refreshedReader);
            }

//            else {
//                // Increase the reference count here so that this reader is not closed.
//                readerCriteriaEntry.getValue().incRef();
//                subReadersMap.put(readerCriteriaEntry.getKey(), readerCriteriaEntry.getValue());
//            }
        }

        if (!newReadersMap.isEmpty()) {
            for (String criteria: originalReadersMap.keySet()) {
                if (!newReadersMap.containsKey(criteria)) {
                    // Increase the reference count here so that this reader is not closed.
                    originalReadersMap.get(criteria).incRef();
                    newReadersMap.put(criteria, originalReadersMap.get(criteria));
                }
            }

            return new OpenSearchMultiReader(referenceToRefresh.getDirectory(), newReadersMap, referenceToRefresh.shardId());
        } else {
            return null;
        }

    }

    @Override
    protected boolean tryIncRef(OpenSearchMultiReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchMultiReader reference) {
        return reference.getRefCount();
    }
}
