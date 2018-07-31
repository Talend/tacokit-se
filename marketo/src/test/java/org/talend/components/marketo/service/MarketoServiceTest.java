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
package org.talend.components.marketo.service;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MARKETO_GUID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SEQ;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_STATUS;

import java.util.Arrays;

import javax.json.JsonObject;
import javax.json.JsonReader;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.LeadAction;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.marketo")
class MarketoServiceTest extends MarketoBaseTest {

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
    }

    @Test
    void getFieldsFromDescribeFormatedForApi() {
        final String fields = "createdAt,externalCompanyId,id,updatedAt,annualRevenue,billingCity,billingCountry,"
                + "billingPostalCode,billingState,billingStreet,company,companyNotes,externalSalesPersonId,industry,"
                + "mainPhone,numberOfEmployees,sicCode,site,website";
        JsonReader reader = jsonReader.createReader(getClass().getClassLoader().getResourceAsStream("describe_company.json"));
        JsonObject v = reader.readObject();
        String f = marketoService
                .getFieldsFromDescribeFormatedForApi(v.getJsonArray("result").get(0).asJsonObject().getJsonArray(ATTR_FIELDS));
        assertEquals(fields, f);
    }

    @ParameterizedTest
    @ValueSource(strings = { "Lead", "List", "Company", "CustomObject", "Opportunity", "OpportunityRole" })
    void testGetInputSchema(String entity) {
        Schema schema = marketoService.getInputSchema(MarketoEntity.valueOf(entity), LeadAction.getLead.name());
        assertNotNull(schema);
    }

    @ParameterizedTest
    @ValueSource(strings = { "Lead", "List" })
    void testLeadSyncOutputSchemas(String entity) {
        Schema schema = marketoService.getOutputSchema(MarketoEntity.valueOf(entity));
        assertNotNull(schema);
        assertFalse(schema.getEntries().isEmpty());
        schema.getEntries().forEach(
                entry -> assertThat(Arrays.asList(ATTR_ID, ATTR_STATUS, ATTR_REASONS), CoreMatchers.hasItem(entry.getName())));
    }

    @ParameterizedTest
    @ValueSource(strings = { "Company", "CustomObject", "Opportunity", "OpportunityRole" })
    void testCustomObjectSyncOutputSchemas(String entity) {
        Schema schema = marketoService.getOutputSchema(MarketoEntity.valueOf(entity));
        assertNotNull(schema);
        assertFalse(schema.getEntries().isEmpty());
        schema.getEntries().forEach(entry -> assertThat(Arrays.asList(ATTR_SEQ, ATTR_MARKETO_GUID, ATTR_STATUS, ATTR_REASONS),
                CoreMatchers.hasItem(entry.getName())));
    }

}
