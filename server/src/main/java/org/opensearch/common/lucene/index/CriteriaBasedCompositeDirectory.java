/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.lucene.store.ByteBuffersDataOutput;
import org.opensearch.lucene.store.ByteBuffersIndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * CriteriaBasedCompositeDirectory
 *
 * @opensearch.api
 */
public class CriteriaBasedCompositeDirectory extends FilterDirectory {

    private final Map<String, Directory> criteriaDirectoryMapping;
    private final Directory multiTenantDirectory;
    private ByteBuffersDataInput currentSegmentInfos = null;
    private final AtomicReference<String> segment_N_name = new AtomicReference<>();

    /**
     * Sole constructor, typically called from sub-classes.
     *
     * @param in
     */
    public CriteriaBasedCompositeDirectory(Directory in, Map<String, Directory> criteriaDirectoryMapping) throws IOException {
        super(in);
        this.multiTenantDirectory = in;
        this.criteriaDirectoryMapping = criteriaDirectoryMapping;
//        SegmentInfos combinedSegmentInfos = Lucene.readSegmentInfos(this);
//        if (combinedSegmentInfos != null) {
//            segment_N_name = "segments";
//            combinedSegmentInfos.commit(this);
//        }
    }

    public Directory getDirectory(String criteria) {
        return criteriaDirectoryMapping.get(criteria);
    }

    public Set<String> getCriteriaList() {
        return criteriaDirectoryMapping.keySet();
    }

    public Map<String, Directory> getCriteriaDirectoryMapping() {
        return criteriaDirectoryMapping;
    }

    // TODO: Handling references of parent IndexWriter for deleting files of child IndexWriter
    // (As of now not removing file in parent delete call). For eg: If a dec ref is called on parent IndexWriter and
    // there are no active references of a file by parent IndexWriter to child IndexWriter, should we delete it?
    @Override
    public void deleteFile(String name) throws IOException {
        if (name.contains("segments")) {
            return;
        }

        // if (name.contains("$")) {
        // String criteria = name.split("\\$")[0];
        // System.out.println("Deleting file from directory " + getDirectory(criteria) + " with name " + name);
        // getDirectory(criteria).deleteFile(name.replace(criteria + "$", ""));
        // } else {
        // System.out.println("Deleting file from directory " + multiTenantDirectory + " with name " + name);
        // multiTenantDirectory.deleteFile(name);
        // }

        // For time being let child IndexWriter take care of deleting files inside it. Parent IndexWriter should only care
        // about deleting files within parent directory.
        if (!name.contains("$")) {
            multiTenantDirectory.deleteFile(name);
        }
    }

    // Fix this.
    @Override
    public String[] listAll() throws IOException {
        // List<String> filesList = new ArrayList<>();
        // for (Map.Entry<String, Directory> filterDirectoryEntry: criteriaDirectoryMapping.entrySet()) {
        // String prefix = filterDirectoryEntry.getKey();
        // Directory filterDirectory = filterDirectoryEntry.getValue();
        // for (String fileName : filterDirectory.listAll()) {
        // filesList.add(prefix + "_" + fileName);
        // }
        // }

        // Exclude group level folder names which is same as criteria
        Set<String> criteriaList = getCriteriaList();
        List<String> filesList = new ArrayList<>(
            Arrays.stream(multiTenantDirectory.listAll()).filter(fileName -> !criteriaList.contains(fileName)).toList()
        );

        // Flatten this for recovery and other flows.
        for (Map.Entry<String, Directory> filterDirectoryEntry: criteriaDirectoryMapping.entrySet()) {
            String prefix = filterDirectoryEntry.getKey();
            Directory filterDirectory = filterDirectoryEntry.getValue();
            for (String fileName : filterDirectory.listAll()) {
                filesList.add(prefix + "_" + fileName);
            }
        }

        if (segment_N_name.get() != null) {
            filesList.add(segment_N_name.get());
        }

        // System.out.println("Parent Directory " + multiTenantDirectory + " list files: " + Arrays.toString(filesList));
        return filesList.toArray(String[]::new);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        if (source.contains("segments")) {
            segment_N_name.compareAndSet(segment_N_name.get(), dest);
        } else {
            multiTenantDirectory.rename(source, dest);
        }
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String name) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).openChecksumInput(name.replace(criteria + "$", ""));
        } else if (name.contains("segments")) {
            // Irrespective of whichever segment_N file we are trying to read the data, we read it from memory buffer. This is assuming we always operate on last IndexCommit.
            return new BufferedChecksumIndexInput(openInput(name, IOContext.READONCE));
        } else {
            return multiTenantDirectory.openChecksumInput(name);
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // segment_N is in memory.
        multiTenantDirectory.sync(names.stream().filter(name -> !(name.contains(segment_N_name.get()) && name.contains("segments"))).collect(Collectors.toSet()));
    }

    // TODO: Select on the basis of filter name.
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            if (getDirectory(criteria) == null) {
                System.err.println("No criteria mapping for " + name + " with criteria " + criteria);
            }

            return getDirectory(criteria).openInput(name.replace(criteria + "$", ""), context);
        } else if (name.contains("segments")) {
            // Irrespective of whichever segment_N file we are trying to read the data, we read it from memory buffer. This is assuming we always operate on last IndexCommit.
            return new ByteBuffersIndexInput((ByteBuffersDataInput) currentSegmentInfos.clone(), name);
        } else {
            return multiTenantDirectory.openInput(name, context);
        }
    }

    // TODO: Merge this
    // TODO: Select on the basis of filter name.
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).createOutput(name.replace(criteria + "$", ""), context);
        } else if (name.contains("segments")) {
            // File name of segments_N should be equal to file name with which it is written.
            segment_N_name.compareAndSet(segment_N_name.get(), name);
            return new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), name, name, new CRC32(), this::onClose);
        } else {
            return multiTenantDirectory.createOutput(name, context);
        }
    }

    // TODO: Select on the basis of filter name.
    @Override
    public long fileLength(String name) throws IOException {
        if (name.contains("$")) {
            String criteria = name.split("\\$")[0];
            return getDirectory(criteria).fileLength(name.replace(criteria + "$", ""));
        } else if (name.contains("segments")) {
            return currentSegmentInfos.length();
        } else {
            return multiTenantDirectory.fileLength(name);
        }
    }

    public List<Directory> getChildDirectoryList() {
        return new ArrayList<>(criteriaDirectoryMapping.values());
    }

    @Override
    public void close() throws IOException {
        for (Directory filterDirectory : criteriaDirectoryMapping.values()) {
            filterDirectory.close();
        }

        multiTenantDirectory.close();
    }

    private synchronized void onClose(ByteBuffersDataOutput output) {
        currentSegmentInfos = output.toDataInput();
    }

    public static CriteriaBasedCompositeDirectory unwrap(Directory directory) {
        while (directory instanceof FilterDirectory && !(directory instanceof CriteriaBasedCompositeDirectory)) {
            directory = ((FilterDirectory) directory).getDelegate();
        }

        if (directory instanceof CriteriaBasedCompositeDirectory) {
            return (CriteriaBasedCompositeDirectory) directory;
        }

        return null;
    }

}
