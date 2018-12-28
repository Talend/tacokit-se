/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.service;

import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.update.Update;

import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UiActionService {

    private static Set<String> MODULE_NOT_SUPPORT_BULK_API = new HashSet<String>(Arrays.asList("AcceptedEventRelation",
            "ActivityHistory", "AggregateResult", "AttachedContentDocument", "CaseStatus", "CombinedAttachment", "ContractStatus",
            "DeclinedEventRelation", "EmailStatus", "LookedUpFromActivity", "Name", "NoteAndAttachment", "OpenActivity",
            "OwnedContentDocument", "PartnerRole", "ProcessInstanceHistory", "RecentlyViewed", "SolutionStatus", "TaskPriority",
            "TaskStatus", "UndecidedEventRelation", "UserRecordAccess"));

    @Service
    private SalesforceService service;

    @Service
    private LocalConfiguration localConfiguration;

    @HealthCheck("basic.healthcheck")
    public HealthCheckStatus validateBasicConnection(@Option final BasicDataStore datastore, final Messages i18n) {
        try {
            this.service.connect(datastore, localConfiguration);
        } catch (ConnectionException ex) {
            String error;
            if (ApiFault.class.isInstance(ex)) {
                final ApiFault fault = ApiFault.class.cast(ex);
                error = fault.getExceptionCode() + " " + fault.getExceptionMessage();
            } else {
                error = ex.getMessage();
            }
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(error));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }

    @Suggestions("loadSalesforceModules")
    public SuggestionValues loadSalesforceModules(@Option("moduleDataSet") final BasicDataStore dataStore) {
        try {
            List<SuggestionValues.Item> items = new ArrayList<>();
            final PartnerConnection connection = this.service.connect(dataStore, localConfiguration);
            DescribeGlobalSObjectResult[] modules = connection.describeGlobal().getSobjects();
            for (DescribeGlobalSObjectResult module : modules) {
                if (!MODULE_NOT_SUPPORT_BULK_API.contains(module.getName())) {
                    items.add(new SuggestionValues.Item(module.getName(), module.getLabel()));
                }
            }
            return new SuggestionValues(true, items);
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        }
    }

    @Update("defaultColumns")
    public ModuleDataSet.ColumnSelectionConfig defaultColumns(@Option("dataStore") final BasicDataStore dataStore,
            @Option("moduleName") final String moduleName) {
        try {
            final ModuleDataSet.ColumnSelectionConfig config = new ModuleDataSet.ColumnSelectionConfig();

            if (moduleName == null || moduleName.isEmpty()) {
                config.setSelectColumnNames(new ArrayList<>());
            } else {
                config.setSelectColumnNames(service.getFieldNameList(dataStore, moduleName, localConfiguration));
            }
            return config;
        } catch (IllegalStateException e) {
            throw e;
        }
    }

}
