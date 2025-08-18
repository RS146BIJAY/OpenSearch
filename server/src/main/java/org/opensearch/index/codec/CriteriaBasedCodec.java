/*Add commentMore actions
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

public class CriteriaBasedCodec extends FilterCodec {

    private final String bucket;
    public static final String BUCKET_NAME = "bucket";

    public CriteriaBasedCodec() {
        super("CriteriaBasedCodec", new Lucene103Codec());
        bucket = null;
    }

    public CriteriaBasedCodec(Codec delegate, String bucket) {
        super("CriteriaBasedCodec", delegate);
        this.bucket = bucket;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                info.putAttribute(BUCKET_NAME, bucket);
                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }
}
