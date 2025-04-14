/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.Term;

public class DeleteEntry {

    private final String id;
    private final long seqNo, primaryTerm, versionOfDeletion;
    private final Term term;
    public DeleteEntry(String id, long seqNo, long primaryTerm, long versionOfDeletion, Term term) {
        this.id = id;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.versionOfDeletion = versionOfDeletion;
        this.term = term;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getVersionOfDeletion() {
        return versionOfDeletion;
    }

    public String getId() {
        return id;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "DeleteEntry{" + "id='" + id + '\'' +
            ", seqNo=" + seqNo +
            ", primaryTerm=" + primaryTerm +
            ", versionOfDeletion=" + versionOfDeletion +
            ", term=" + term +
            '}';
    }
}
