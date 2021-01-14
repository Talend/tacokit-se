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

import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.talend.components.salesforce.configuration.InputModuleConfig;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.service.Messages;
import org.talend.components.salesforce.service.SalesforceService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.FILE_SALESFORCE)
@Emitter(name = "ModuleQueryInput")
@Documentation("Salesforce module query input ")
public class ModuleQueryEmitter extends AbstractQueryEmitter implements Serializable {

    private transient List<String> allModuleFields;

    public ModuleQueryEmitter(@Option("configuration") final InputModuleConfig inputModuleConfig, final SalesforceService service,
            LocalConfiguration configuration, final RecordBuilderFactory recordBuilderFactory, final Messages messages) {
        super(inputModuleConfig, service, configuration, recordBuilderFactory, messages);
    }

    /**
     * Build SOQL based on selected columns and module name
     */
    public String getQuery() {

        List<String> selectedColumns = getColumnNames();
        List<String> queryFields;
        if (selectedColumns == null || selectedColumns.isEmpty()) {
            queryFields = getAllModuleFields();
        } else if (!getAllModuleFields().containsAll(selectedColumns)) { // ensure requested fields exist
            throw new IllegalStateException(
                    "columns { " + selectedColumns.stream().filter(c -> !getAllModuleFields().contains(c)).collect(joining(","))
                            + " } " + "doesn't exist in module '" + getModuleName() + "'");
        } else {
            queryFields = selectedColumns;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        int count = 0;
        for (String se : queryFields) {
            if (count++ > 0) {
                sb.append(", ");
            }
            sb.append(se);
        }
        sb.append(" from ");
        sb.append(getModuleName());
        if (((ModuleDataSet) inputConfig.getDataSet()).getCondition() != null
                && !((ModuleDataSet) inputConfig.getDataSet()).getCondition().isEmpty()) {
            sb.append(" where ");
            sb.append(((ModuleDataSet) inputConfig.getDataSet()).getCondition());
        }
        return sb.toString();
    }

    @Override
    String getModuleName() {
        return ((ModuleDataSet) inputConfig.getDataSet()).getModuleName();
    }

    @Override
    List<String> getColumnNames() {
        List<String> selectedFields = ((ModuleDataSet) inputConfig.getDataSet()).getSelectColumnNames();
        if (selectedFields != null && selectedFields.size() > 0) {
            return ((ModuleDataSet) inputConfig.getDataSet()).getSelectColumnNames();
        } else {
            return getAllModuleFields();
        }
    }

    private List<String> getAllModuleFields() {
        if (allModuleFields == null) {
            DescribeSObjectResult describeSObjectResult;
            try {
                final PartnerConnection connection = service.connect(inputConfig.getDataSet().getDataStore(), localConfiguration);
                describeSObjectResult = connection.describeSObject(getModuleName());
                allModuleFields = new ArrayList<>();
                for (Field field : describeSObjectResult.getFields()) {
                    // filter the invalid compound columns for salesforce bulk query api
                    if (!service.isSuppotedType(field)) {
                        continue;
                    }
                    allModuleFields.add(field.getName());
                }
                return allModuleFields;
            } catch (ConnectionException e) {
                if (ApiFault.class.isInstance(e)) {
                    ApiFault fault = ApiFault.class.cast(e);
                    throw new IllegalStateException(fault.getExceptionMessage(), e);
                }
                throw new IllegalStateException(e);
            }
        }
        return allModuleFields;
    }

}
