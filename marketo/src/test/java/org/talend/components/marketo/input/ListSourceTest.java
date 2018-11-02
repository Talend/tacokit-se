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

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.ListAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class ListSourceTest extends SourceBaseTest {

    private ListSource source;

    private Integer LIST_ID = 1001;

    private String LEAD_IDS = "1,2,3,4,5";

    private String fields = "company,site,billingStreet,billingCity,billingState,billingCountry,billingPostalCode,website,mainPhone,annualRevenue,numberOfEmployees,industry,sicCode,mktoCompanyNotes,externalCompanyId,id,mktoName,personType,mktoIsPartner,isLead,mktoIsCustomer,isAnonymous,salutation,firstName,middleName,lastName,email,phone,mobilePhone,fax,title,contactCompany,dateOfBirth,address,city,state,country,postalCode,personTimeZone,originalSourceType,originalSourceInfo,registrationSourceType,registrationSourceInfo,originalSearchEngine,originalSearchPhrase,originalReferrer,emailInvalid,emailInvalidCause,unsubscribed,unsubscribedReason,doNotCall,mktoDoNotCallCause,doNotCallReason,mktoPersonNotes,anonymousIP,inferredCompany,inferredCountry,inferredCity,inferredStateRegion,inferredPostalCode,inferredMetropolitanArea,inferredPhoneAreaCode,department,createdAt,updatedAt,cookies,externalSalesPersonId,leadPerson,leadRole,leadSource,leadStatus,leadScore,urgency,priority,relativeScore,relativeUrgency,rating,personPrimaryLeadInterest,leadPartitionId,leadRevenueCycleModelId,leadRevenueStageId,gender,facebookDisplayName,twitterDisplayName,linkedInDisplayName,facebookProfileURL,twitterProfileURL,linkedInProfileURL,facebookPhotoURL,twitterPhotoURL,linkedInPhotoURL,facebookReach,twitterReach,linkedInReach,facebookReferredVisits,twitterReferredVisits,linkedInReferredVisits,totalReferredVisits,facebookReferredEnrollments,twitterReferredEnrollments,linkedInReferredEnrollments,totalReferredEnrollments,lastReferredVisit,lastReferredEnrollment,syndicationId,facebookId,twitterId,linkedInId,acquisitionProgramId,mktoAcquisitionDate";

    private transient static final Logger LOG = LoggerFactory.getLogger(ListSourceTest.class);

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.List);
    }

    @Test
    void testGetLists() {
        inputDataSet.setListAction(ListAction.list);
        inputDataSet.setLeadIds("");
        inputDataSet.setListName("");
        inputDataSet.setProgramName("");
        inputDataSet.setWorkspaceName("");
        inputDataSet.setBatchSize(3);
        source = new ListSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertThat(result.getString(ATTR_NAME), CoreMatchers.containsString("List00"));
        }
    }

    @Test
    void testGetListById() {
        inputDataSet.setListAction(ListAction.get);
        inputDataSet.setListId(LIST_ID);
        source = new ListSource(inputDataSet, service, tools);
        source.init();
        result = source.next();
        assertNotNull(result);
        assertEquals(result.getString(ATTR_NAME), "GroupList000");
        assertEquals(result.getInt(ATTR_ID), 1001);
        result = source.next();
        assertNull(result);
    }

    @Test
    void testGetLeadsByListId() {
        inputDataSet.setListAction(ListAction.getLeads);
        inputDataSet.setListId(LIST_ID);
        inputDataSet.setLeadIds(LEAD_IDS);
        inputDataSet.setBatchSize(2);
        inputDataSet.setFields(asList(fields.split(",")));
        source = new ListSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testIsMemberOfList() {
        inputDataSet.setListAction(ListAction.isMemberOf);
        inputDataSet.setListId(LIST_ID);
        inputDataSet.setLeadIds(LEAD_IDS);
        source = new ListSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }
}
