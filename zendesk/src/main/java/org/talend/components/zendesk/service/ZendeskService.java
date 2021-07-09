/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.zendesk.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.source.ZendeskInputMapperConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import java.util.List;

@Service
@Slf4j
public class ZendeskService {

    @Service
    private Messages i18n;

    @Service
    private HealthChecker healthChecker;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private SchemaDiscoverer schemaDiscoverer;

    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID)
    public Schema guessTableSchema(final ZendeskDataSet dataSet) {
        log.debug("guess schema");
        final ZendeskInputMapperConfiguration configuration = new ZendeskInputMapperConfiguration();
        configuration.setDataset(dataSet);
        List<String> columns = schemaDiscoverer.getColumns(configuration);
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        columns.stream().forEach(k -> {
            Schema.Entry schemaEntry = recordBuilderFactory.newEntryBuilder().withName(k).withType(Schema.Type.STRING).build();
            schemaBuilder.withEntry(schemaEntry);
        });
        return schemaBuilder.build();
    }

    @HealthCheck(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
    public HealthCheckStatus validateBasicConnection(@Option final ZendeskDataStore dataStore) {
        try {
            log.debug("health check");
            healthChecker.checkHealth(dataStore);
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.healthCheckOk());
    }
}