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
package org.talend.components.salesforce.input;

import java.io.Serializable;
import java.util.List;

import org.talend.components.salesforce.configuration.InputSOQLConfig;
import org.talend.components.salesforce.dataset.SOQLQueryDataSet;
import org.talend.components.salesforce.service.Messages;
import org.talend.components.salesforce.service.SalesforceService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.FILE_SALESFORCE)
@Emitter(name = "SOQLQueryInput")
@Documentation("Salesforce soql query input ")
public class SOQLQueryEmitter extends AbstractQueryEmitter implements Serializable {

    public SOQLQueryEmitter(@Option("configuration") final InputSOQLConfig inputSOQLConfig, final SalesforceService service,
            LocalConfiguration configuration, final RecordBuilderFactory recordBuilderFactory, final Messages messages) {
        super(inputSOQLConfig, service, configuration, recordBuilderFactory, messages);
    }

    @Override
    String getQuery() {
        return ((SOQLQueryDataSet) inputConfig.getDataSet()).getQuery();
    }

    /**
     * Extract module name from SOQL
     */
    @Override
    public String getModuleName() {
        return SalesforceService.guessModuleName(getQuery());
    }

    /**
     * Extract column list from SOQL
     */
    @Override
    List<String> getColumnNames() {
        return SalesforceService.guessColumnNamesFromSOQL(getQuery());
    }

}
