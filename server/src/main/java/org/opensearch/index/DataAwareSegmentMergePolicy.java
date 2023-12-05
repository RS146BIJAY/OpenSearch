/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.LocalTime;
import java.util.*;

/**
 * Wrapper around {@link TieredMergePolicy} which doesn't respect
 * {@link TieredMergePolicy#setMaxMergedSegmentMB(double)} on forced merges, but DOES respect it on only_expunge_deletes.
 * See https://issues.apache.org/jira/browse/LUCENE-7976.
 *
 * @opensearch.internal
 */
public class DataAwareSegmentMergePolicy extends TieredMergePolicy {
    /**
     * Creates a new filter merge policy instance wrapping another.
     *
     */
    public DataAwareSegmentMergePolicy() {
        super();
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        try {
            final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
            MergeSpecification spec = null;
            final Map<Integer, List<SegmentCommitInfo>> commitInfos = new HashMap<>();
            for (SegmentCommitInfo si : infos) {
                if (merging.contains(si)) {
                    continue;
                }

                final PointsReader pointsReader = getPointReader(si);
                byte[] dateInBytes = pointsReader.getValues("@timestamp").getMinPackedValue();
                long dateInMills = LongPoint.decodeDimension(dateInBytes, 0);

                LocalDateTime timeOfDay = Instant.ofEpochSecond(dateInMills)
                    .atOffset(ZoneOffset.UTC)
                    .toLocalDateTime();

                int day = timeOfDay.getDayOfYear();
                commitInfos.computeIfAbsent(day, k -> new ArrayList<>()).add(si);
            }

            for (int day: commitInfos.keySet()) {
                if (commitInfos.get(day).size() > 1) {
                    if (spec == null) {
                        spec = new MergeSpecification();
                    }

                    spec.add(new OneMerge(commitInfos.get(day)));
                }
            }

            return spec;
        } catch (Exception ex) {
            return super.findMerges(mergeTrigger, infos, mergeContext);
        }
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
        return findMerges(null, infos, mergeContext);
    }

    private PointsReader getPointReader(SegmentCommitInfo si) throws IOException {
        final Codec codec = si.info.getCodec();
        final Directory dir = si.info.dir;
        final Directory cfsDir;
        if (si.info.getUseCompoundFile()) {
            cfsDir = codec.compoundFormat().getCompoundReader(dir, si.info, IOContext.READONCE);
        } else {
            cfsDir = dir;
        }

        final FieldInfos coreFieldInfos = codec.fieldInfosFormat().read(cfsDir, si.info, "", IOContext.READONCE);
        final SegmentReadState segmentReadState =
            new SegmentReadState(cfsDir, si.info, coreFieldInfos, IOContext.READONCE);

        return codec.pointsFormat().fieldsReader(segmentReadState);
    }
}
