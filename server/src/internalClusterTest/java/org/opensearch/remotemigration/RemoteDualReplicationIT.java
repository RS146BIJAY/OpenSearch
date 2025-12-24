/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.IndexService;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.engine.CombinedDocValues;
import org.opensearch.index.engine.MissingHistoryOperationsException;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteDualReplicationIT extends MigrationBaseTestCase {
    private final String REMOTE_PRI_DOCREP_REP = "remote-primary-docrep-replica";
    private final String REMOTE_PRI_DOCREP_REMOTE_REP = "remote-primary-docrep-remote-replica";
    private final String FAILOVER_REMOTE_TO_DOCREP = "failover-remote-to-docrep";
    private final String FAILOVER_REMOTE_TO_REMOTE = "failover-remote-to-remote";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /* Adding the following mock plugins:
        - InternalSettingsPlugin : To override default intervals of retention lease and global ckp sync
        - MockFsRepositoryPlugin and MockTransportService.TestPlugin: To ensure remote interactions are not no-op and retention leases are properly propagated
         */
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(InternalSettingsPlugin.class, MockFsRepositoryPlugin.class, MockTransportService.TestPlugin.class)
        ).collect(Collectors.toList());
    }

    /*
    Scenario:
    - Starts 2 docrep backed node
    - Creates index with 1 replica
    - Index some docs
    - Start 1 remote backed node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Index some more docs
    - Assert primary-replica consistency
     */
    public void testRemotePrimaryDocRepReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep data nodes");
        internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 1 replica");
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
        createIndex(REMOTE_PRI_DOCREP_REP, oneReplica);
        ensureGreen(REMOTE_PRI_DOCREP_REP);

        int initialBatch = randomIntBetween(1, 1000);
        logger.info("---> Indexing {} docs", initialBatch);
        indexBulk(REMOTE_PRI_DOCREP_REP, initialBatch);

        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 remote enabled data node");
        setAddRemote(true);
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        String primaryShardHostingNode = primaryNodeName(REMOTE_PRI_DOCREP_REP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(REMOTE_PRI_DOCREP_REP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(REMOTE_PRI_DOCREP_REP);
        ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        String primaryShardHostingNodeId = clusterState.getRoutingTable()
            .index(REMOTE_PRI_DOCREP_REP)
            .shard(0)
            .primaryShard()
            .currentNodeId();
        assertTrue(clusterState.getNodes().get(primaryShardHostingNodeId).isRemoteStoreNode());

        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing another {} docs", secondBatch);
        indexBulk(REMOTE_PRI_DOCREP_REP, secondBatch);
        // Defensive check to ensure that doc count in replica shard catches up to the primary copy
        refreshAndWaitForReplication(REMOTE_PRI_DOCREP_REP);
        assertReplicaAndPrimaryConsistency(REMOTE_PRI_DOCREP_REP, initialBatch, secondBatch);
    }

    /*
    Scenario:
    - Starts 2 docrep backed data node
    - Creates an index with 1 replica
    - Starts 1 remote backed data node
    - Index some docs
    - Move primary copy from docrep to remote through _cluster/reroute
    - Starts another remote backed data node
    - Expands index to 2 replicas. One replica copy lies in remote backed node and other in docrep backed node
    - Index some more docs
    - Assert primary-replica consistency
     */
    public void testRemotePrimaryDocRepAndRemoteReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep data nodes");
        internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 1 replica");
        Settings zeroReplicas = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
        createIndex(REMOTE_PRI_DOCREP_REMOTE_REP, zeroReplicas);
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);
        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 remote enabled data node");
        setAddRemote(true);

        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        int firstBatch = randomIntBetween(1, 100);
        logger.info("---> Indexing {} docs", firstBatch);
        indexBulk(REMOTE_PRI_DOCREP_REMOTE_REP, firstBatch);

        String primaryShardHostingNode = primaryNodeName(REMOTE_PRI_DOCREP_REMOTE_REP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(REMOTE_PRI_DOCREP_REMOTE_REP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);
        ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        String primaryShardHostingNodeId = clusterState.getRoutingTable()
            .index(REMOTE_PRI_DOCREP_REMOTE_REP)
            .shard(0)
            .primaryShard()
            .currentNodeId();
        assertTrue(clusterState.getNodes().get(primaryShardHostingNodeId).isRemoteStoreNode());

        logger.info("---> Starting another remote enabled node");
        internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        logger.info("---> Expanding index to 2 replica copies");
        Settings twoReplicas = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices(REMOTE_PRI_DOCREP_REMOTE_REP)
                .setSettings(twoReplicas)
                .get()
        );
        ensureGreen(REMOTE_PRI_DOCREP_REMOTE_REP);

        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing another {} docs", secondBatch);
        indexBulk(REMOTE_PRI_DOCREP_REMOTE_REP, secondBatch);
        // Defensive check to ensure that doc count in replica shard catches up to the primary copy
        refreshAndWaitForReplication(REMOTE_PRI_DOCREP_REMOTE_REP);
        assertReplicaAndPrimaryConsistency(REMOTE_PRI_DOCREP_REMOTE_REP, firstBatch, secondBatch);
    }

    /*
    Checks if retention leases are published on primary shard and it's docrep copies, but not on remote copies
     */
    public void testRetentionLeasePresentOnDocrepReplicaButNotRemote() throws Exception {
        /* Reducing indices.memory.shard_inactive_time to force a flush and trigger translog sync,
        instead of relying on Global CKP Sync action which doesn't run on remote enabled copies

        Under steady state, RetentionLeases would be on (GlobalCkp + 1) on a
        docrep enabled shard copy and (GlobalCkp) for a remote enabled shard copy.
        This is because we block translog sync on remote enabled shard copies during the GlobalCkpSync background task.

        RLs on remote enabled copies are brought up to (GlobalCkp + 1) upon a flush request issued by IndexingMemoryController
        when the shard becomes inactive after SHARD_INACTIVE_TIME_SETTING interval.

        Flush triggers a force sync of translog which bumps the RetentionLease sequence number along with it
        */
        extraSettings = Settings.builder().put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), "3s").build();
        testRemotePrimaryDocRepAndRemoteReplica();
        pollAndCheckRetentionLeases(REMOTE_PRI_DOCREP_REMOTE_REP);
    }

    /*
    Scenario:
    - Starts 2 docrep backed data node
    - Creates an index with 1 replica
    - Starts 1 remote backed data node
    - Index some docs
    - Move primary copy from docrep to remote through _cluster/reroute
    - Starts another remote backed data node
    - Expands index to 2 replicas. One replica copy lies in remote backed node and other in docrep backed node
    - Index some more docs
    - Assert retention lease consistency
     */
    public void testMissingRetentionLeaseCreatedOnFailedOverRemoteReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep data nodes");
        internalCluster().startDataOnlyNodes(2);

        Settings zeroReplicasAndOverridenSyncIntervals = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            .build();
        createIndex(FAILOVER_REMOTE_TO_REMOTE, zeroReplicasAndOverridenSyncIntervals);
        ensureGreen(FAILOVER_REMOTE_TO_REMOTE);

        indexBulk(FAILOVER_REMOTE_TO_REMOTE, 100);

        logger.info("---> Starting first remote node");
        initDocRepToRemoteMigration();
        addRemote = true;
        String firstRemoteNode = internalCluster().startDataOnlyNode();
        String primaryShardHostingNode = primaryNodeName(FAILOVER_REMOTE_TO_REMOTE);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, firstRemoteNode);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_REMOTE, 0, primaryShardHostingNode, firstRemoteNode))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_REMOTE);
        assertReplicaAndPrimaryConsistency(FAILOVER_REMOTE_TO_REMOTE, 100, 0);

        String secondRemoteNode = internalCluster().startDataOnlyNode();
        Settings twoReplicas = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices(FAILOVER_REMOTE_TO_REMOTE)
                .setSettings(twoReplicas)
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_REMOTE);

        logger.info("---> Checking retention leases");
        pollAndCheckRetentionLeases(FAILOVER_REMOTE_TO_REMOTE);
    }

    private void pollAndCheckRetentionLeases(String indexName) throws Exception {
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        assertBusy(() -> {
            for (ShardStats shardStats : internalCluster().client().admin().indices().prepareStats(indexName).get().getShards()) {
                ShardRouting shardRouting = shardStats.getShardRouting();
                DiscoveryNode discoveryNode = nodes.get(shardRouting.currentNodeId());
                RetentionLeases retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases();
                if (shardRouting.primary()) {
                    // Primary copy should be on remote node and should have retention leases
                    assertTrue(discoveryNode.isRemoteStoreNode());
                    assertCheckpointsConsistency(shardStats);
                    assertRetentionLeaseConsistency(shardStats, retentionLeases);
                } else {
                    // Checkpoints and Retention Leases are not synced to remote replicas
                    if (discoveryNode.isRemoteStoreNode()) {
                        assertTrue(shardStats.getRetentionLeaseStats().retentionLeases().leases().isEmpty());
                    } else {
                        // Replica copy on docrep node should have retention leases
                        assertCheckpointsConsistency(shardStats);
                        assertRetentionLeaseConsistency(shardStats, retentionLeases);
                    }
                }
            }
        });
    }

    /*
    Scenario:
    - Starts 2 docrep backed data node
    - Creates an index with 1 replica
    - Starts 1 remote backed data node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Stops remote enabled node
    - Ensure doc count is same after failover
    - Index some more docs to ensure working of failed-over primary
     */
    public void testFailoverRemotePrimaryToDocrepReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep data nodes");
        internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 0 replica");
        Settings excludeRemoteNode = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        createIndex(FAILOVER_REMOTE_TO_DOCREP, excludeRemoteNode);
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);
        initDocRepToRemoteMigration();
        logger.info("---> Starting 1 remote enabled data node");
        setAddRemote(true);
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        logger.info("---> Starting doc ingestion in parallel thread");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(FAILOVER_REMOTE_TO_DOCREP);
        asyncIndexingService.startIndexing();

        String primaryShardHostingNode = primaryNodeName(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_DOCREP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        logger.info("---> Expanding index to 2 replica copies");
        Settings twoReplicas = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices(FAILOVER_REMOTE_TO_DOCREP)
                .setSettings(twoReplicas)
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Stopping indexing thread");
        asyncIndexingService.stopIndexing();

        refreshAndWaitForReplication(FAILOVER_REMOTE_TO_DOCREP);
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
            .admin()
            .indices()
            .prepareStats(FAILOVER_REMOTE_TO_DOCREP)
            .setDocs(true)
            .get()
            .asMap();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        long initialPrimaryDocCount = 0;
        for (ShardRouting shardRouting : shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertTrue(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                initialPrimaryDocCount = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        int firstBatch = (int) asyncIndexingService.getIndexedDocs();
        assertReplicaAndPrimaryConsistency(FAILOVER_REMOTE_TO_DOCREP, firstBatch, 0);

        logger.info("---> Stop remote store enabled node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName));
        ensureStableCluster(3);
        ensureYellow(FAILOVER_REMOTE_TO_DOCREP);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        long primaryDocCountAfterFailover = 0;
        for (ShardRouting shardRouting : shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertFalse(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                primaryDocCountAfterFailover = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        assertEquals(initialPrimaryDocCount, primaryDocCountAfterFailover);

        logger.info("---> Index some more docs to ensure that the failed over primary is ingesting new docs");
        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing {} more docs", secondBatch);
        indexBulk(FAILOVER_REMOTE_TO_DOCREP, secondBatch);
        refreshAndWaitForReplication(FAILOVER_REMOTE_TO_DOCREP);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_DOCREP).setDocs(true).get().asMap();
        assertEquals(2, shardStatsMap.size());
        shardStatsMap.forEach(
            (shardRouting, shardStats) -> { assertEquals(firstBatch + secondBatch, shardStats.getStats().getDocs().getCount()); }
        );
    }

    /*
    Scenario:
    - Starts 2 docrep backed data nodes
    - Creates an index with 1 replica
    - Starts 1 remote backed data node
    - Moves primary copy from docrep to remote through _cluster/reroute
    - Starts 1 more remote backed data node
    - Expands index to 2 replicas, one each on new remote node and docrep node
    - Stops remote enabled node hosting the primary
    - Ensures remote replica gets promoted to primary
    - Ensures doc count is same after failover
    - Indexes some more docs to ensure working of failed-over primary
    */
    public void testFailoverRemotePrimaryToRemoteReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        logger.info("---> Starting 2 docrep data nodes");
        List<String> docrepNodeNames = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();
        assertEquals(internalCluster().client().admin().cluster().prepareGetRepositories().get().repositories().size(), 0);

        logger.info("---> Creating index with 1 replica");
        createIndex(FAILOVER_REMOTE_TO_REMOTE, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureGreen(FAILOVER_REMOTE_TO_REMOTE);
        initDocRepToRemoteMigration();

        logger.info("---> Starting 1 remote enabled data node");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        logger.info("---> Starting doc ingestion in parallel thread");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(FAILOVER_REMOTE_TO_REMOTE);
        asyncIndexingService.startIndexing();

        String primaryNodeName = primaryNodeName(FAILOVER_REMOTE_TO_REMOTE);
        logger.info("---> Moving primary copy from docrep node {} to remote enabled node {}", primaryNodeName, remoteNodeName1);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_REMOTE, 0, primaryNodeName, remoteNodeName1))
                .get()
        );
        waitForRelocation();
        ensureGreen(FAILOVER_REMOTE_TO_REMOTE);
        assertEquals(primaryNodeName(FAILOVER_REMOTE_TO_REMOTE), remoteNodeName1);

        logger.info("---> Starting 1 more remote enabled data node");
        String remoteNodeName2 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();

        logger.info("---> Expanding index to 2 replica copies, on docrepNode and remoteNode2");
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices(FAILOVER_REMOTE_TO_REMOTE)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                        // prevent replica copy from being allocated to the extra docrep node
                        .put("index.routing.allocation.exclude._name", primaryNodeName)
                        .build()
                )
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_REMOTE);

        logger.info("---> Stopping indexing thread");
        asyncIndexingService.stopIndexing();

        refreshAndWaitForReplication(FAILOVER_REMOTE_TO_REMOTE);
        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
            .admin()
            .indices()
            .prepareStats(FAILOVER_REMOTE_TO_REMOTE)
            .setDocs(true)
            .get()
            .asMap();
        DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        long initialPrimaryDocCount = 0;
        for (ShardRouting shardRouting : shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertTrue(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                initialPrimaryDocCount = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        int firstBatch = (int) asyncIndexingService.getIndexedDocs();
        assertReplicaAndPrimaryConsistency(FAILOVER_REMOTE_TO_REMOTE, firstBatch, 0);

        logger.info("---> Stop remote store enabled node hosting the primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName1));
        ensureStableCluster(4);
        ensureYellowAndNoInitializingShards(FAILOVER_REMOTE_TO_REMOTE);
        DiscoveryNodes finalNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();

        waitUntil(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            String nodeId = clusterState.getRoutingTable().index(FAILOVER_REMOTE_TO_REMOTE).shard(0).primaryShard().currentNodeId();
            if (nodeId == null) {
                return false;
            } else {
                assertEquals(finalNodes.get(nodeId).getName(), remoteNodeName2);
                return finalNodes.get(nodeId).isRemoteStoreNode();
            }
        });

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_REMOTE).setDocs(true).get().asMap();
        long primaryDocCountAfterFailover = 0;
        for (ShardRouting shardRouting : shardStatsMap.keySet()) {
            if (shardRouting.primary()) {
                assertTrue(finalNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                primaryDocCountAfterFailover = shardStatsMap.get(shardRouting).getStats().getDocs().getCount();
            }
        }
        assertEquals(initialPrimaryDocCount, primaryDocCountAfterFailover);

        logger.info("---> Index some more docs to ensure that the failed over primary is ingesting new docs");
        int secondBatch = randomIntBetween(1, 10);
        logger.info("---> Indexing {} more docs", secondBatch);
        indexBulk(FAILOVER_REMOTE_TO_REMOTE, secondBatch);
        refreshAndWaitForReplication(FAILOVER_REMOTE_TO_REMOTE);

        shardStatsMap = internalCluster().client().admin().indices().prepareStats(FAILOVER_REMOTE_TO_REMOTE).setDocs(true).get().asMap();
        assertEquals(2, shardStatsMap.size());
        shardStatsMap.forEach(
            (shardRouting, shardStats) -> { assertEquals(firstBatch + secondBatch, shardStats.getStats().getDocs().getCount()); }
        );
    }

    /*
    Scenario:
    - Starts 1 docrep backed data node
    - Creates an index with 0 replica
    - Starts 1 remote backed data node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Stops remote enabled node
    - Ensure doc count is same after failover
    - Index some more docs to ensure working of failed-over primary
    - Starts another remote node
    - Move primary copy from docrep to remote through _cluster/reroute
    - Ensure that remote store is seeded in the new remote node by asserting remote uploads from that node > 0
     */
    public void testFailoverRemotePrimaryToDocrepReplicaReseedToRemotePrimary() throws Exception {
        testFailoverRemotePrimaryToDocrepReplica();

        logger.info("---> Removing replica copy");
        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings(FAILOVER_REMOTE_TO_DOCREP)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        logger.info("---> Starting a new remote enabled node");
        setAddRemote(true);
        String remoteNodeName = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        String primaryShardHostingNode = primaryNodeName(FAILOVER_REMOTE_TO_DOCREP);
        logger.info("---> Moving primary copy from {} to remote enabled node {}", primaryShardHostingNode, remoteNodeName);
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(FAILOVER_REMOTE_TO_DOCREP, 0, primaryShardHostingNode, remoteNodeName))
                .get()
        );
        ensureGreen(FAILOVER_REMOTE_TO_DOCREP);

        Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
            .admin()
            .indices()
            .prepareStats(FAILOVER_REMOTE_TO_DOCREP)
            .get()
            .asMap();
        DiscoveryNodes discoveryNodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
        assertEquals(1, shardStatsMap.size());
        shardStatsMap.forEach((shardRouting, shardStats) -> {
            if (discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode()) {
                RemoteSegmentStats remoteSegmentStats = shardStats.getStats().getSegments().getRemoteSegmentStats();
                assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
            }
        });
    }

    public void testValidate() throws Exception {
        // For RemoteDualReplicationIT_79B8DB1F187B42C1-001
        // t1: Term list is size 114 doc count 315
        // t3: Term list is size 113 doc count 111
        // t4: Term list is size 113 doc count 111

        // For RemoteDualReplicationIT_A19B9881B3958250-001
        // t2: Term list is size 126 doc count 369
        // t3: Term list is size 130 doc count 129
        // t4: Term list is size 130 doc count 129 terms

        // t4: Term list is size 122 doc count 120 terms
        // t2: Term list is size 122 doc count 336 terms

        String indexPath = "/Users/rissag/OpenSearch/OpenSearch/server/build/testrun/internalClusterTest/temp/org.opensearch.remotemigration.RemoteDualReplicationIT_EA1E400C5C826EDF-001/tempDir-002/node_t2/nodes/0/indices/wQs32FXwSv-aCV_iaWEmUw/0/index";
        Directory directory = FSDirectory.open(Paths.get(indexPath));

        List<String> termsList = new ArrayList<>();
        int docCount = 0;
        try (DirectoryReader directoryReader = DirectoryReader.open(directory);) {
            for (LeafReaderContext ctx : directoryReader.leaves()) {
                TermsEnum iterator = ctx.reader().terms("_id").iterator();
                BytesRef term;
                AtomicReference<TermsEnum> termsEnum = new AtomicReference<>(iterator);
                AtomicReference<PostingsEnum> docEnum = new AtomicReference<>(null);
                while ((term = iterator.next()) != null) {
                    termsList.add(term.toString());
                }

                docCount += ctx.reader().numDocs();
            }

            System.out.println("Term list is size " + termsList.size() + " doc count " + docCount + " terms " + Arrays.toString(termsList.toArray()));
        }
    }

    public void testValidateActualDocs() throws Exception {
        String indexPath = "/Users/rissag/OpenSearch/OpenSearch/server/build/testrun/internalClusterTest/temp/org.opensearch.remotemigration.RemoteDualReplicationIT_EA1E400C5C826EDF-001/tempDir-002/node_t3/nodes/0/indices/wQs32FXwSv-aCV_iaWEmUw/0/index";
        Directory directory = FSDirectory.open(Paths.get(indexPath));
        try (DirectoryReader directoryReader = Lucene.wrapAllDocsLive(DirectoryReader.open(directory))) {
            IndexSearcher searcher = new IndexSearcher(directoryReader);
            final TopDocs topDocs = searchOperations(null, true, searcher);
            ParallelArray parallelArray = new ParallelArray(1024);
            fillParallelArray(topDocs.scoreDocs, parallelArray, searcher);
            int i = 0;
            AtomicLong lastSeqNo = new AtomicLong(Integer.MIN_VALUE);
            int skippedNo = 0;
            while (i < topDocs.scoreDocs.length) {
                Translog.Operation op = readDocAsOp(i, parallelArray, topDocs.scoreDocs, lastSeqNo);
                ++i;
                if (op != null) {
                    lastSeqNo.set(op.seqNo());
                    System.out.println(op);
                } else {
                    ++skippedNo;
                }
            }

            System.out.println("Skipped no " + skippedNo);
        }
    }

    private TopDocs searchOperations(FieldDoc after, boolean accurate, IndexSearcher indexSearcher) throws IOException {
        final Query rangeQuery = operationsRangeQuery(0, Integer.MAX_VALUE);
        final Sort sortedBySeqNo = new Sort(new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG));
        final TopFieldCollector topFieldCollector = new TopFieldCollectorManager(
            sortedBySeqNo,
            1024,
            after,
            accurate ? Integer.MAX_VALUE : 0,
            false
        ).newCollector();
        indexSearcher.search(rangeQuery, topFieldCollector);
        return topFieldCollector.topDocs();
    }

    public static Query operationsRangeQuery(long fromSeqNo, long toSeqNo) {
        return new BooleanQuery.Builder().add(LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, fromSeqNo, toSeqNo), BooleanClause.Occur.MUST)
            .add(Queries.newNonNestedFilter(), BooleanClause.Occur.MUST) // exclude non-root nested docs
            .build();
    }

    private Translog.Operation readDocAsOp(int docIndex, ParallelArray parallelArray, ScoreDoc[] scoreDocs, AtomicLong lastSeenSeqNo) throws IOException {
        final LeafReaderContext leaf = parallelArray.leafReaderContexts[docIndex];
        final int segmentDocID = scoreDocs[docIndex].doc - leaf.docBase;
        final long primaryTerm = parallelArray.primaryTerm[docIndex];
        assert primaryTerm > 0 : "nested child document must be excluded";
        final long seqNo = parallelArray.seqNo[docIndex];
        // Only pick the first seen seq#
        if (seqNo == lastSeenSeqNo.get()) {
            return null;
        }
        final long version = parallelArray.version[docIndex];
        final String sourceField = parallelArray.hasRecoverySource[docIndex]
            ? SourceFieldMapper.RECOVERY_SOURCE_NAME
            : SourceFieldMapper.NAME;
        final FieldsVisitor fields = new FieldsVisitor(true, sourceField);
        leaf.reader().storedFields().document(segmentDocID, fields);

        final Translog.Operation op;
        final boolean isTombstone = parallelArray.isTombStone[docIndex];
        if (isTombstone && fields.id() == null) {
            op = new Translog.NoOp(seqNo, primaryTerm, fields.source().utf8ToString());
            assert version == 1L : "Noop tombstone should have version 1L; actual version [" + version + "]";
        } else {
            final String id = fields.id();
            if (isTombstone) {
                op = new Translog.Delete(id, seqNo, primaryTerm, version);
            } else {
                final BytesReference source = fields.source();
                if (source == null) {
                    // TODO: Callers should ask for the range that source should be retained. Thus we should always
                    // check for the existence source once we make peer-recovery to send ops after the local checkpoint.
                    return null;
                }
                // TODO: pass the latest timestamp from engine.
                final long autoGeneratedIdTimestamp = -1;
                op = new Translog.Index(
                    id,
                    seqNo,
                    primaryTerm,
                    version,
                    source.toBytesRef().bytes,
                    fields.routing(),
                    autoGeneratedIdTimestamp
                );
            }
        }

        return op;
    }

    private static final class ParallelArray {
        final LeafReaderContext[] leafReaderContexts;
        final long[] version;
        final long[] seqNo;
        final long[] primaryTerm;
        final boolean[] isTombStone;
        final boolean[] hasRecoverySource;

        ParallelArray(int size) {
            version = new long[size];
            seqNo = new long[size];
            primaryTerm = new long[size];
            isTombStone = new boolean[size];
            hasRecoverySource = new boolean[size];
            leafReaderContexts = new LeafReaderContext[size];
        }
    }

    private void fillParallelArray(ScoreDoc[] scoreDocs, ParallelArray parallelArray, IndexSearcher indexSearcher) throws IOException {
        if (scoreDocs.length > 0) {
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i].shardIndex = i;
            }
            // for better loading performance we sort the array by docID and
            // then visit all leaves in order.
            ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(i -> i.doc));
            int docBase = -1;
            int maxDoc = 0;
            List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
            int readerIndex = 0;
            CombinedDocValues combinedDocValues = null;
            LeafReaderContext leaf = null;
            for (ScoreDoc scoreDoc : scoreDocs) {
                if (scoreDoc.doc >= docBase + maxDoc) {
                    do {
                        leaf = leaves.get(readerIndex++);
                        docBase = leaf.docBase;
                        maxDoc = leaf.reader().maxDoc();
                    } while (scoreDoc.doc >= docBase + maxDoc);
                    combinedDocValues = new CombinedDocValues(leaf.reader());
                }
                final int segmentDocID = scoreDoc.doc - docBase;
                final int index = scoreDoc.shardIndex;
                parallelArray.leafReaderContexts[index] = leaf;
                parallelArray.seqNo[index] = combinedDocValues.docSeqNo(segmentDocID);
                parallelArray.primaryTerm[index] = combinedDocValues.docPrimaryTerm(segmentDocID);
                parallelArray.version[index] = combinedDocValues.docVersion(segmentDocID);
                parallelArray.isTombStone[index] = combinedDocValues.isTombstone(segmentDocID);
                parallelArray.hasRecoverySource[index] = combinedDocValues.hasRecoverySource(segmentDocID);
            }
            // now sort back based on the shardIndex. we use this to store the previous index
            ArrayUtil.introSort(scoreDocs, Comparator.comparingInt(i -> i.shardIndex));
        }
    }

    private int getDocID(BytesRef id, LeafReaderContext context, AtomicReference<TermsEnum> termsEnum, AtomicReference<PostingsEnum> docsEnum) throws IOException {
        // termsEnum can possibly be null here if this leaf contains only no-ops.
        if (termsEnum.get() != null && termsEnum.get().seekExact(id)) {
            final Bits liveDocs = context.reader().getLiveDocs();
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            // there may be more than one matching docID, in the case of nested docs, so we want the last one:
            docsEnum.set(termsEnum.get().postings(docsEnum.get(), 0));
            for (int d = docsEnum.get().nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.get().nextDoc()) {
                if (liveDocs != null && liveDocs.get(d) == false) {
                    continue;
                }
                docID = d;
            }
            return docID;
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    private static long readNumericDocValues(LeafReader reader, String field, int docId) throws IOException {
        final NumericDocValues dv = reader.getNumericDocValues(field);
        if (dv == null || dv.advanceExact(docId) == false) {
            assert false : "document [" + docId + "] does not have docValues for [" + field + "]";
            throw new IllegalStateException("document [" + docId + "] does not have docValues for [" + field + "]");
        }
        return dv.longValue();
    }

    /*
    Performs the same experiment as testRemotePrimaryDocRepReplica.

    This ensures that the primary shard for the index has moved over to remote
    enabled node whereas the replica copy is still left behind on the docrep nodes

    At this stage, segrep lag computation shouldn't consider the docrep shard copy while calculating bytes lag
     */
    public void testZeroSegrepLagForShardsWithMixedReplicationGroup() throws Exception {
        testRemotePrimaryDocRepReplica();
        String remoteNodeName = internalCluster().client()
            .admin()
            .cluster()
            .prepareNodesStats()
            .get()
            .getNodes()
            .stream()
            .filter(nodeStats -> nodeStats.getNode().isRemoteStoreNode())
            .findFirst()
            .get()
            .getNode()
            .getName();
        ReplicationStats replicationStats = internalCluster().client()
            .admin()
            .cluster()
            .prepareNodesStats(remoteNodeName)
            .get()
            .getNodes()
            .get(0)
            .getIndices()
            .getSegments()
            .getReplicationStats();
        assertEquals(0, replicationStats.getMaxBytesBehind());
        assertEquals(0, replicationStats.getTotalBytesBehind());
        assertEquals(0, replicationStats.getMaxReplicationLag());
    }

    private void assertReplicaAndPrimaryConsistency(String indexName, int firstBatch, int secondBatch) throws Exception {
        try {
            assertBusy(() -> {
                refresh(FAILOVER_REMOTE_TO_REMOTE);
                Map<ShardRouting, ShardStats> shardStatsMap = internalCluster().client()
                    .admin()
                    .indices()
                    .prepareStats(indexName)
                    .setDocs(true)
                    .get()
                    .asMap();
                DiscoveryNodes nodes = internalCluster().client().admin().cluster().prepareState().get().getState().getNodes();
                for (ShardRouting shardRouting : shardStatsMap.keySet()) {
                    CommonStats shardStats = shardStatsMap.get(shardRouting).getStats();
                    if (shardRouting.primary()) {
                        assertEquals(firstBatch + secondBatch, shardStats.getDocs().getCount());
                        assertTrue(nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
                        RemoteSegmentStats remoteSegmentStats = shardStats.getSegments().getRemoteSegmentStats();
                        assertTrue(remoteSegmentStats.getUploadBytesSucceeded() > 0);
                        assertTrue(remoteSegmentStats.getTotalUploadTime() > 0);
                    }
                    if (shardRouting.unassigned() == false && shardRouting.primary() == false) {
                        boolean remoteNode = nodes.get(shardRouting.currentNodeId()).isRemoteStoreNode();
                        assertEquals(
                            "Mismatched doc count. Is this on remote node ? " + remoteNode,
                            firstBatch + secondBatch,
                            shardStats.getDocs().getCount()
                        );
                        RemoteSegmentStats remoteSegmentStats = shardStats.getSegments().getRemoteSegmentStats();
                        if (remoteNode) {
                            assertTrue(remoteSegmentStats.getDownloadBytesStarted() > 0);
                            assertTrue(remoteSegmentStats.getTotalDownloadTime() > 0);
                        } else {
                            assertEquals(0, remoteSegmentStats.getUploadBytesSucceeded());
                            assertEquals(0, remoteSegmentStats.getTotalUploadTime());
                        }
                    }
                }
            });
        } catch (Error| Exception e) {
            client().admin().indices().prepareFlush().setForce(true).execute().actionGet();
            AtomicLong co = new AtomicLong();
            while (true && co.get() >= 0) {
                if (co.get() <= 2) {
                    System.out.println("Start Debugging");
                    co.incrementAndGet();
                }
            }

            throw e;
        }

    }

    /**
     * For a docrep enabled shard copy or a primary shard copy,
     * asserts that the stored Retention Leases equals to 1 + maxSeqNo ingested on the node
     *
     * @param shardStats ShardStats object from NodesStats API
     * @param retentionLeases RetentionLeases from NodesStats API
     */
    private static void assertRetentionLeaseConsistency(ShardStats shardStats, RetentionLeases retentionLeases) {
        long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
        for (RetentionLease rl : retentionLeases.leases()) {
            assertEquals(maxSeqNo + 1, rl.retainingSequenceNumber());
        }
    }

    /**
     * For a docrep enabled shard copy or a primary shard copy,
     * asserts that local and global checkpoints are up-to-date with maxSeqNo of doc operations
     *
     * @param shardStats ShardStats object from NodesStats API
     */
    private static void assertCheckpointsConsistency(ShardStats shardStats) {
        long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
        long localCkp = shardStats.getSeqNoStats().getLocalCheckpoint();
        long globalCkp = shardStats.getSeqNoStats().getGlobalCheckpoint();

        assertEquals(maxSeqNo, localCkp);
        assertEquals(maxSeqNo, globalCkp);
    }
}
