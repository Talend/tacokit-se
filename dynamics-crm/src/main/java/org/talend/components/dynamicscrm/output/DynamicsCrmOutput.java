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
package org.talend.components.dynamicscrm.output;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.naming.AuthenticationException;
import javax.naming.ServiceUnavailableException;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.talend.components.dynamicscrm.output.DynamicsCrmOutputConfiguration.Action;
import org.talend.components.dynamicscrm.service.DynamicsCrmException;
import org.talend.components.dynamicscrm.service.DynamicsCrmService;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.components.dynamicscrm.service.PropertyValidationData;
import org.talend.ms.crm.odata.DynamicsCRMClient;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

@Version(1)
@Icon(value = IconType.CUSTOM, custom = "azure-dynamics")
@Processor(name = "AzureDynamics365Output")
@Documentation("Azure Dynamics 365 output")
public class DynamicsCrmOutput implements Serializable {

    private final DynamicsCrmOutputConfiguration configuration;

    private final DynamicsCrmService service;

    private DynamicsCRMClient client;

    private Edm metadata;

    private I18n i18n;

    private List<String> fields;

    private EdmEntitySet entitySet;

    private RecordProcessor processor;

    public DynamicsCrmOutput(@Option("configuration") final DynamicsCrmOutputConfiguration configuration,
            final DynamicsCrmService service, final I18n i18n) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        try {
            client = service.createClient(configuration.getDataset().getDatastore(), configuration.getDataset().getEntitySet());
        } catch (AuthenticationException e) {
            throw new DynamicsCrmException(i18n.authenticationFailed(e.getMessage()));
        }
        metadata = service.getMetadata(client);
        entitySet = metadata.getEntityContainer().getEntitySet(configuration.getDataset().getEntitySet());
        Set<String> possibleColumns = service
                .getPropertiesValidationData(client, configuration.getDataset().getDatastore(),
                        entitySet.getEntityType().getName())
                .stream().filter(getFilter()).map(PropertyValidationData::getName).collect(Collectors.toSet());

        List<String> columnNames = configuration.getColumns();
        if (columnNames == null || columnNames.isEmpty()) {
            columnNames = entitySet.getEntityType().getPropertyNames();
        }
        fields = columnNames.stream().filter(s -> possibleColumns.contains(client.extractNavigationLinkName(s)))
                .collect(Collectors.toList());
        processor = createProcessor(configuration.getAction());
    }

    private Predicate<? super PropertyValidationData> getFilter() {
        switch (configuration.getAction()) {
        case INSERT:
            return PropertyValidationData::isValidForCreate;
        case UPSERT:
            return PropertyValidationData::isValidForUpdate;
        default:
            return t -> true;
        }
    }

    private RecordProcessor createProcessor(Action action) {
        switch (action) {
        case DELETE:
            return new DeleteRecordProcessor(client, entitySet, i18n);
        case UPSERT:
            return new UpsertRecordProcessor(client, i18n, entitySet, configuration, metadata, fields);
        case INSERT:
            return new InsertRecordProcessor(client, i18n, entitySet, configuration, metadata, fields);
        default:
            throw new IllegalArgumentException();
        }
    }

    @ElementListener
    public void onNext(@Input final Record defaultInput) {
        try {
            processor.processRecord(defaultInput);
        } catch (ServiceUnavailableException e) {
            throw new DynamicsCrmException(i18n.failedToInsertEntity(e.getMessage()), e);
        }
    }

    @PreDestroy
    public void release() {
        client = null;
    }
}