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
import java.util.*;

/**
 * Wrapper around {@link TieredMergePolicy} which doesn't respect
 * {@link TieredMergePolicy#setMaxMergedSegmentMB(double)} on forced merges, but DOES respect it on only_expunge_deletes.
 * See https://issues.apache.org/jira/browse/LUCENE-7976.
 *
 * @opensearch.internal
 */
public class DataAwareSegmentMergePolicy extends TieredMergePolicy {

    private long maxMergedSegmentBytes = 1024 * 1024 * 1024L;

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
            return getMergeSpecification(infos, mergeContext);
        } catch (Exception ex) {
            return super.findMerges(mergeTrigger, infos, mergeContext);
        }
    }

    private MergeSpecification getMergeSpecification(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        MergeSpecification spec = null;
        List<SegmentInfoDayHourSize> sortedByDay = getSortedByDay(infos, mergeContext);
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();

        final Map<String, List<SegmentCommitInfo>> commitInfos = new HashMap<>();
        long[] daySize = new long[367];
        Iterator<SegmentInfoDayHourSize> iter = sortedByDay.iterator();
        while (iter.hasNext()) {
            SegmentInfoDayHourSize siDayHourSize = iter.next();
            if (merging.contains(siDayHourSize.segInfo) || siDayHourSize.sizeInBytes >= maxMergedSegmentBytes) {
                iter.remove();
            } else {
                daySize[siDayHourSize.day] += siDayHourSize.sizeInBytes;
            }
        }

        iter = sortedByDay.iterator();
        int currentDay = -1;
        List<SegmentCommitInfo> commitInfoList = new ArrayList<>();
        while (iter.hasNext()) {
            SegmentInfoDayHourSize segmentInfoDayHourSize = iter.next();
            if (merging.contains(segmentInfoDayHourSize.segInfo) || segmentInfoDayHourSize.sizeInBytes >= maxMergedSegmentBytes) {
                continue;
            }

            if (currentDay != segmentInfoDayHourSize.day && currentDay != -1) {
                if (commitInfoList.size() > 1) {
                    if (spec == null) {
                        spec = new MergeSpecification();
                    }

                    spec.add(new OneMerge(commitInfoList));
                }

                commitInfoList = new ArrayList<>();
            }

            currentDay = segmentInfoDayHourSize.day;
            if (daySize[currentDay] >= maxMergedSegmentBytes) {
                commitInfoList.add(segmentInfoDayHourSize.segInfo);
                iter.remove();
            }
        }

        if (commitInfoList.size() > 1) {
            if (spec == null) {
                spec = new MergeSpecification();
            }

            spec.add(new OneMerge(commitInfoList));
        }

        iter = sortedByDay.iterator();
        while (iter.hasNext()) {
            SegmentInfoDayHourSize siDayHourSize = iter.next();
            if (merging.contains(siDayHourSize.segInfo)) {
                continue;
            }

            commitInfos.computeIfAbsent(String.valueOf(siDayHourSize.day) + "," + siDayHourSize.hour, k -> new ArrayList<>()).add(siDayHourSize.segInfo);
        }

        for (String dayHour: commitInfos.keySet()) {
            if (commitInfos.get(dayHour).size() > 1) {
                if (spec == null) {
                    spec = new MergeSpecification();
                }

                spec.add(new OneMerge(commitInfos.get(dayHour)));
            }
        }

        return spec;
    }

    private List<SegmentInfoDayHourSize> getSortedByDay(SegmentInfos infos, MergeContext context) throws IOException {
        List<SegmentInfoDayHourSize> sortedByDay = new ArrayList<>();
        for (SegmentCommitInfo info : infos) {
            LocalDateTime timeOfDay = getTimeOfDay(info);
            sortedByDay.add(new SegmentInfoDayHourSize(info, getDay(timeOfDay), getHour(timeOfDay), size(info, context)));
        }

        sortedByDay.sort(
            (info1, info2) -> {
                int cmp = Long.compare(info1.day, info2.day);
                if (cmp == 0) {
                    cmp = Long.compare(info1.hour, info2.hour);
                }

                return cmp;
            });

        return sortedByDay;
    }

    private LocalDateTime getTimeOfDay(final SegmentCommitInfo si) throws IOException {
        final PointsReader pointsReader = getPointReader(si);
        byte[] dateInBytes = pointsReader.getValues("@timestamp").getMinPackedValue();

        // Is this analysizer specific?
        long dateInMills = LongPoint.decodeDimension(dateInBytes, 0);
        return Instant.ofEpochMilli(dateInMills)
            .atOffset(ZoneOffset.UTC)
            .toLocalDateTime();
    }

    // For hour we use max packed value to get the ending hour of the array.
    private int getHour(final LocalDateTime timeOfDay)  {
        return timeOfDay.getHour();
    }

    private int getDay(final LocalDateTime timeOfDay) throws IOException {
        return timeOfDay.getDayOfYear();
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

    private static class SegmentInfoDayHourSize {
        private final SegmentCommitInfo segInfo;
        /// Size of the segment in bytes, pro-rated by the number of live documents.
        private final int hour;
        private final int day;
        private final long sizeInBytes;

        SegmentInfoDayHourSize(SegmentCommitInfo info, final int day, final int hour, final long sizeInBytes) {
            segInfo = info;
            this.day = day;
            this.hour = hour;
            this.sizeInBytes = sizeInBytes;
        }
    }
}
