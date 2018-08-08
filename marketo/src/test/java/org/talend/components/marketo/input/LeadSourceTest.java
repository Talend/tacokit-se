// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.input;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EMAIL;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

import java.util.Collections;
import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.LeadAction;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
public class LeadSourceTest extends SourceBaseTest {

    static final String LEAD_IDS_KEY_VALUES = "1,2,3,4,5";

    private String LEAD_EMAIL = "Marketo@talend.com";

    private Integer LEAD_ID = 5;

    private Integer INVALID_LEAD_ID = -5;

    private LeadSource source;

    private String fields = "company,site,billingStreet,billingCity,billingState,billingCountry,billingPostalCode,website,mainPhone,annualRevenue,numberOfEmployees,industry,sicCode,mktoCompanyNotes,externalCompanyId,id,mktoName,personType,mktoIsPartner,isLead,mktoIsCustomer,isAnonymous,salutation,firstName,middleName,lastName,email,phone,mobilePhone,fax,title,contactCompany,dateOfBirth,address,city,state,country,postalCode,personTimeZone,originalSourceType,originalSourceInfo,registrationSourceType,registrationSourceInfo,originalSearchEngine,originalSearchPhrase,originalReferrer,emailInvalid,emailInvalidCause,unsubscribed,unsubscribedReason,doNotCall,mktoDoNotCallCause,doNotCallReason,mktoPersonNotes,anonymousIP,inferredCompany,inferredCountry,inferredCity,inferredStateRegion,inferredPostalCode,inferredMetropolitanArea,inferredPhoneAreaCode,department,createdAt,updatedAt,cookies,externalSalesPersonId,leadPerson,leadRole,leadSource,leadStatus,leadScore,urgency,priority,relativeScore,relativeUrgency,rating,personPrimaryLeadInterest,leadPartitionId,leadRevenueCycleModelId,leadRevenueStageId,gender,facebookDisplayName,twitterDisplayName,linkedInDisplayName,facebookProfileURL,twitterProfileURL,linkedInProfileURL,facebookPhotoURL,twitterPhotoURL,linkedInPhotoURL,facebookReach,twitterReach,linkedInReach,facebookReferredVisits,twitterReferredVisits,linkedInReferredVisits,totalReferredVisits,facebookReferredEnrollments,twitterReferredEnrollments,linkedInReferredEnrollments,totalReferredEnrollments,lastReferredVisit,lastReferredEnrollment,syndicationId,facebookId,twitterId,linkedInId,acquisitionProgramId,mktoAcquisitionDate";

    private transient static final Logger LOG = LoggerFactory.getLogger(LeadSourceTest.class);

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.Lead);
        inputDataSet.setBatchSize(200);
    }

    @Test
    void testDescribeLead() {
        inputDataSet.setLeadAction(LeadAction.describeLead);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
        result = source.runAction();
        assertNotNull(result);
        assertEquals(fields, marketoService.getFieldsFromDescribeFormatedForApi(result.getJsonArray(ATTR_RESULT)));
    }

    @Test
    void testGetActivities() {
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.getAccessToken();
        JsonObject activities = source.getActivities();
        assertTrue(activities.getJsonArray("result").size() > 30);
    }

    @Test
    void testGetLead() {
        inputDataSet.setLeadAction(LeadAction.getLead);
        inputDataSet.setLeadId(LEAD_ID);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        result = source.next();
        assertNotNull(result);
        assertEquals(LEAD_EMAIL, result.getString(ATTR_EMAIL));
        assertNull(source.next());
    }

    @Test
    void testGetLeadNotFound() {
        inputDataSet.setLeadAction(LeadAction.getLead);
        inputDataSet.setLeadId(INVALID_LEAD_ID);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        assertNull(source.next());
    }

    @Test
    void testGetMultipleLeads() {
        setMultipleLeadsDefault();
        inputDataSet.setBatchSize(2);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
        // will all fields
        inputDataSet.setFields(fields);
        inputDataSet.setBatchSize(2);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetMultipleLeadsWithAllFields() {
        setMultipleLeadsDefault();
        inputDataSet.setFields(fields);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetMultipleLeadsWithAllFieldsOver8k() {
        setMultipleLeadsDefault();
        inputDataSet.setFields(fields + String.join(" ", Collections.nCopies(5000, " ")));
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetMultipleLeadsWithUnknownField() {
        setMultipleLeadsDefault();
        inputDataSet.setFields("unknownField");
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        try {
            source.init();
            fail("[1006] Field 'unknownField' not found -> should have been raised");
        } catch (Exception e) {
        }
    }

    @Test
    void testGetMultipleLeadsWithPager() {
        setMultipleLeadsDefault();
        inputDataSet.setBatchSize(1);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    private void setMultipleLeadsDefault() {
        inputDataSet.setLeadAction(LeadAction.getMultipleLeads);
        inputDataSet.setLeadKeyName(ATTR_ID);
        inputDataSet.setLeadKeyValues(LEAD_IDS_KEY_VALUES);
    }

    @Test
    void testGetLeadChanges() {
        inputDataSet.setLeadAction(LeadAction.getLeadChanges);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        inputDataSet.setFields(fields);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetLeadChangesWithPaging() {
        inputDataSet.setLeadAction(LeadAction.getLeadChanges);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        inputDataSet.setFields(fields);
        inputDataSet.setBatchSize(5);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetLeadActivities() {
        inputDataSet.setLeadAction(LeadAction.getLeadActivity);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        SuggestionValues acts = uiActionService.getActivities(inputDataSet);
        List<String> activities = activities = acts.getItems().stream().limit(10).map(item -> item.getId()).collect(toList());
        LOG.debug("[testGetLeadActivities] activities: {}", activities);
        inputDataSet.setActivityTypeIds(activities);
        inputDataSet.setFields(fields);
        inputDataSet.setBatchSize(300);
        source = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }
}
