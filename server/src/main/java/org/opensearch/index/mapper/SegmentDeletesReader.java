/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.Locale;

public class SegmentDeletesReader {

    public static void main(String[] args) throws IOException {
        String indexPath = "/hdd1/mnt/env/root/apollo/env/swift-eu-west-1-staging-OS_3_3AMI-ES2-p001/var/es/data/nodes/0/indices/_lqqeHTjQUu0YinB8tREWg/0/index";
        printSegmentDeletes(indexPath);
    }

    public static void printSegmentDeletes(String indexPath) throws IOException {
        Directory directory = FSDirectory.open(Paths.get(indexPath));

        try {
            // Read the latest segment infos
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);

            System.out.println("=== Lucene Index Segment Analysis ===");
            System.out.println("Index Path: " + indexPath);
            System.out.println("Number of Segments: " + segmentInfos.size());
            System.out.println("Version: " + segmentInfos.getVersion());
            System.out.println();

            NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
            nf.setMaximumFractionDigits(2);

            int totalDocs = 0;
            int totalDeletedDocs = 0;
            int totalMaxDocs = 0;

            // Iterate through each segment
            for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
                SegmentInfo segmentInfo = segmentCommitInfo.info;

                int maxDoc = segmentInfo.maxDoc();
                int delCount = segmentCommitInfo.getDelCount();
                int numDocs = maxDoc - delCount;
                double deletePercentage = maxDoc > 0 ? (delCount * 100.0 / maxDoc) : 0.0;

                totalMaxDocs += maxDoc;
                totalDeletedDocs += delCount;
                totalDocs += numDocs;

                System.out.println("----------------------------------------");
                System.out.println("Segment Name: " + segmentInfo.name);
                System.out.println("  Max Docs: " + nf.format(maxDoc));
                System.out.println("  Live Docs: " + nf.format(numDocs));
                System.out.println("  Deleted Docs (Hard Deletes): " + nf.format(delCount));
                System.out.println("  Delete Percentage: " + nf.format(deletePercentage) + "%");
                System.out.println("  Segment Size: " + nf.format(segmentCommitInfo.sizeInBytes() / (1024.0 * 1024.0)) + " MB");
                System.out.println("  Has Deletions: " + segmentCommitInfo.hasDeletions());
                System.out.println("  Del Gen: " + segmentCommitInfo.getDelGen());
                System.out.println("  Codec: " + segmentInfo.getCodec().getName());
                System.out.println("  Version: " + segmentInfo.getVersion());
            }

            // Print summary
            System.out.println("========================================");
            System.out.println("=== Summary ===");
            System.out.println("Total Segments: " + segmentInfos.size());
            System.out.println("Total Max Docs: " + nf.format(totalMaxDocs));
            System.out.println("Total Live Docs: " + nf.format(totalDocs));
            System.out.println("Total Deleted Docs: " + nf.format(totalDeletedDocs));
            System.out.println("Overall Delete Percentage: " +
                nf.format(totalMaxDocs > 0 ? (totalDeletedDocs * 100.0 / totalMaxDocs) : 0.0) + "%");
            System.out.println("========================================");

        } finally {
            directory.close();
        }
    }

}
