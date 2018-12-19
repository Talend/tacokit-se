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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.salesforce.service.SalesforceService.URL;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.SalesforceTestBase;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

@Disabled("Salesforce credentials is not ready on ci")
@WithComponents("org.talend.components.salesforce")
public class UiActionServiceTest extends SalesforceTestBase {

    @Service
    private UiActionService service;

    @Service
    private Messages i18n;

    @Test
    @DisplayName("Test connection OK [Valid]")
    public void validateBasicConnectionOK() {
        final HealthCheckStatus status = service.validateBasicConnection(getDataStore(), i18n);
        assertEquals(i18n.healthCheckOk(), status.getComment());
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());

    }

    @Test
    @DisplayName("Test connection Failed [Invalid]")
    public void validateBasicConnectionFailed() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        final HealthCheckStatus status = service.validateBasicConnection(datasore, i18n);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @DisplayName("Test load modules [Valid]")
    public void loadModules() {
        final SuggestionValues modules = service.loadSalesforceModules(getDataStore());
        assertNotNull(modules);
        assertTrue(modules.isCacheable());
        final List<String> moduleNames = new ArrayList<>();
        modules.getItems().stream().forEach(e -> moduleNames.add(e.getId()));
        assertTrue("Return module name list is not include all expected modules!", moduleNames.containsAll(asList("Account",
                "AccountContactRole", "AccountFeed", "AccountHistory", "AccountPartner", "AccountShare", "AdditionalNumber",
                "ApexClass", "ApexComponent", "ApexLog", "ApexPage", "ApexTrigger", "Asset", "AssetFeed", "AssignmentRule",
                "AsyncApexJob", "Attachment", "BrandTemplate", "BusinessHours", "BusinessProcess", "CallCenter", "Campaign",
                "CampaignFeed", "CampaignMember", "CampaignMemberStatus", "CampaignShare", "Case", "CaseComment",
                "CaseContactRole", "CaseFeed", "CaseHistory", "CaseShare", "CaseSolution", "CaseTeamMember", "CaseTeamRole",
                "CaseTeamTemplate", "CaseTeamTemplateMember", "CaseTeamTemplateRecord", "CategoryData", "CategoryNode",
                "CollaborationGroup", "CollaborationGroupFeed", "CollaborationGroupMember", "Community", "Contact", "ContactFeed",
                "ContactHistory", "ContactShare", "Contract", "ContractContactRole", "ContractFeed", "ContractHistory",
                "CronTrigger", "Document", "DocumentAttachmentMap", "EmailServicesAddress", "EmailServicesFunction",
                "EmailTemplate", "EntitySubscription", "Event", "FeedComment", "FeedTrackedChange", "FiscalYearSettings",
                "Folder", "ForecastShare", "Group", "GroupMember", "Holiday", "Idea", "IdeaComment", "Lead", "LeadFeed",
                "LeadHistory", "LeadShare", "LeadStatus", "MailmergeTemplate", "Note", "Opportunity", "OpportunityCompetitor",
                "OpportunityContactRole", "OpportunityFeed", "OpportunityFieldHistory", "OpportunityHistory",
                "OpportunityLineItem", "OpportunityPartner", "OpportunityShare", "OpportunityStage", "OrgWideEmailAddress",
                "Organization", "Partner", "Period", "Pricebook2", "PricebookEntry", "ProcessInstance", "ProcessInstanceStep",
                "ProcessInstanceWorkitem", "Product2", "Product2Feed", "Profile", "QueueSobject", "RecordType", "Scontrol",
                "Site", "SiteHistory", "Solution", "SolutionFeed", "SolutionHistory", "StaticResource", "User", "UserFeed",
                "UserLicense", "UserPreference", "UserRole", "Vote", "WebLink")));
    }

    @Test
    @DisplayName("Test connection with bad credentials [Invalid]")
    public void loadModulesWithBadCredentials() {
        assertThrows(IllegalStateException.class, () -> {
            final BasicDataStore datasore = new BasicDataStore();
            datasore.setEndpoint(URL);
            datasore.setUserId("basUserName");
            datasore.setPassword("NoPass");
            service.loadSalesforceModules(datasore);
        });
    }

    @Test
    @DisplayName("Test retrieve column names [Valid]")
    public void retrieveColumnsName() {
        final String moduleName = "Account";
        ModuleDataSet.ColumnSelectionConfig filedNameList = service.defaultColumns(getDataStore(), moduleName);
        assertNotNull(filedNameList);
        assertNotNull(filedNameList.getSelectColumnNames());
        assertTrue("Return module field name list is not include all expected fields!",
                filedNameList.getSelectColumnNames()
                        .containsAll(asList("Id", "IsDeleted", "MasterRecordId", "Name", "Type", "ParentId", "BillingStreet",
                                "BillingCity", "BillingState", "BillingPostalCode", "BillingCountry", "BillingLatitude",
                                "BillingLongitude", "ShippingStreet", "ShippingCity", "ShippingState", "ShippingPostalCode",
                                "ShippingCountry", "ShippingLatitude", "ShippingLongitude", "Phone", "Fax", "AccountNumber",
                                "Website", "PhotoUrl", "Sic", "Industry", "AnnualRevenue", "NumberOfEmployees", "Ownership",
                                "TickerSymbol", "Description", "Rating", "Site", "OwnerId", "CreatedDate", "CreatedById",
                                "LastModifiedDate", "LastModifiedById", "SystemModstamp", "LastActivityDate", "LastViewedDate",
                                "LastReferencedDate", "Jigsaw", "JigsawCompanyId", "AccountSource", "SicDesc")));

    }

}
