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
package org.talend.components.dynamicscrm.source;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.naming.AuthenticationException;

import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.talend.components.dynamicscrm.service.DynamicsCrmException;
import org.talend.components.dynamicscrm.service.DynamicsCrmService;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.components.dynamicscrm.service.PropertyValidationData;
import org.talend.ms.crm.odata.DynamicsCRMClient;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Documentation("Dynamics CRM input source")
public class DynamicsCrmInputSource implements Serializable {

    private final DynamicsCrmInputMapperConfiguration configuration;

    private final DynamicsCrmService service;

    private final RecordBuilderFactory builderFactory;

    private DynamicsCRMClient client;

    private DynamicsCrmQueryResultsIterator iterator;

    @Service
    private I18n i18n;

    private Schema schema;

    private Edm metadata;

    private final InputHelper helper;

    public DynamicsCrmInputSource(@Option("configuration") final DynamicsCrmInputMapperConfiguration configuration,
            final DynamicsCrmService service, final RecordBuilderFactory builderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.helper = new InputHelper(i18n);
    }

    @PostConstruct
    public void init() {
        try {
            client = service.createClient(configuration.getDataset().getDatastore(), configuration.getDataset().getEntitySet());
        } catch (AuthenticationException e) {
            throw new DynamicsCrmException(i18n.authenticationFailed(e.getMessage()));
        }

        metadata = service.getMetadata(client);
        EdmEntitySet entitySet = metadata.getEntityContainer().getEntitySet(configuration.getDataset().getEntitySet());
        Set<String> readableColumns = service
                .getPropertiesValidationData(client, configuration.getDataset().getDatastore(),
                        entitySet.getEntityType().getName())
                .stream().filter(PropertyValidationData::isValidForRead).map(PropertyValidationData::getName)
                .collect(Collectors.toSet());

        List<String> columnNames = configuration.getColumns();
        if (columnNames == null || columnNames.isEmpty()) {
            columnNames = entitySet.getEntityType().getPropertyNames();
        }
        columnNames = columnNames.stream().filter(s -> readableColumns.contains(client.extractNavigationLinkName(s)))
                .collect(Collectors.toList());
        schema = helper.getSchemaFromMetadata(metadata, configuration.getDataset().getEntitySet(), columnNames, builderFactory);
        iterator = service.getEntitySetIterator(client, helper.createQueryOptionConfig(schema, configuration));
    }

    @Producer
    public Record next() {
        if (iterator.hasNext()) {
            ClientEntity next = iterator.next();
            return helper.createRecord(next, schema, builderFactory);
        }
        return null;
    }

    @PreDestroy
    public void release() {
        iterator = null;
    }
}