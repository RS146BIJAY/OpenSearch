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

package org.opensearch.indices.mapping;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Priority;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.bucket.SignificantTermsSignificanceScoreIT;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SimpleGetMappingsIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(InternalSettingsPlugin.class)
        ).collect(Collectors.toSet());
    }

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
        assertThat(response.mappings().containsKey("index"), equalTo(true));
        assertEmptyOrOnlyDefaultMappings(response, "index");
    }

    private void assertEmptyOrOnlyDefaultMappings(GetMappingsResponse response, String indexName) {
        final Map<String, MappingMetadata> mappings = response.mappings();
        MappingMetadata indexMappings = mappings.get(indexName);
        if (indexMappings.getSourceAsMap().containsKey("context_aware_grouping")) {
            assertEquals(indexMappings.getSourceAsMap().size(), 2);
            assertTrue(indexMappings.getSourceAsMap().containsKey("dynamic"));
        } else {
            assertEquals(indexMappings, MappingMetadata.EMPTY_MAPPINGS);
        }
    }

    private XContentBuilder getMappingForType() throws IOException {
        return jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
    }

    public void testSimpleGetMappings() throws Exception {
        client().admin().indices().prepareCreate("indexa").setMapping(getMappingForType()).execute().actionGet();
        client().admin().indices().prepareCreate("indexb").setMapping(getMappingForType()).execute().actionGet();

        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // Get all mappings
        GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa"), notNullValue());
        assertThat(response.mappings().get("indexb"), notNullValue());

        // Get all mappings, via wildcard support
        response = client().admin().indices().prepareGetMappings("*").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa"), notNullValue());
        assertThat(response.mappings().get("indexb"), notNullValue());

        // Get mappings in indexa
        response = client().admin().indices().prepareGetMappings("indexa").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("indexa"), notNullValue());
    }

    public void testGetMappingsWithBlocks() throws IOException {
        client().admin().indices().prepareCreate("test").setMapping(getMappingForType()).execute().actionGet();
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
                assertThat(response.mappings().size(), equalTo(1));
                assertNotNull(response.mappings().get("test"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetMappings(), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
