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

import static org.junit.Assert.*;

import java.util.List;
import javax.json.JsonObject;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.marketo")
class CompanySourceTest extends SourceBaseTest {

    CompanySource source;

    final String fields = "createdAt,externalCompanyId,id,updatedAt,annualRevenue,billingCity,billingCountry,"
            + "billingPostalCode,billingState,billingStreet,company,companyNotes,externalSalesPersonId,industry,"
            + "mainPhone,numberOfEmployees,sicCode,site,website";

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.Company);
    }

    @Test
    void testDescribeCompanies() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        source = new CompanySource(inputDataSet, i18n, authorizationClient, companyClient);
        source.init();
        JsonObject result = source.next();
        assertNotNull(result);
        assertEquals(fields, marketoService.getFieldsFromDescribeFormatedForApi(result.getJsonArray("fields")));
        result = source.next();
        assertNull(result);
    }

    @Test
    void testGetCompanies() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
        source = new CompanySource(inputDataSet, i18n, authorizationClient, companyClient);
        source.init();
        JsonObject json;
        while ((json = source.next()) != null) {
            assertNotNull(json);
            Assert.assertThat(json.getString("externalCompanyId"), CoreMatchers.containsString("google0"));
        }
    }

    @Test
    void testGetCompaniesFails() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("billingCountry");
        inputDataSet.setFilterValues("France");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
        source = new CompanySource(inputDataSet, i18n, authorizationClient, companyClient);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] Invalid filterType 'billingCountry'", e.getMessage());
        }
    }

    @Test
    public void testDescribeCompaniesPipeline() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        // We convert our configuration instance to URI configuration
        final String uriConfig = SimpleFactory.configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        // We create our job test pipeline
        Job.components() //
                .component("company", "Marketo://Input?" + uriConfig) //
                .component("collector", "test://collector") //
                .connections() //
                .from("company") //
                .to("collector") //
                .build() //
                .run();
        //
        final List<JsonObject> res = component.getCollectedData(JsonObject.class);
        assertEquals(1, res.size());
    }

    @Test
    void testGetCompaniesPipeline() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(fields);
        // We convert our configuration instance to URI configuration
        final String uriConfig = SimpleFactory.configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        // We create our job test pipeline
        Job.components() //
                .component("company", "Marketo://Input?" + uriConfig) //
                .component("collector", "test://collector") //
                .connections() //
                .from("company") //
                .to("collector") //
                .build() //
                .run();
        //
        final List<JsonObject> res = component.getCollectedData(JsonObject.class);
        assertEquals(5, res.size());
        JsonObject record = res.get(0).asJsonObject();
        assertEquals("google01", record.getString("externalCompanyId"));
        assertEquals(JSON_VALUE_XUNDEFINED_X, record.getString("industry", JSON_VALUE_XUNDEFINED_X));
    }

    @Test
    void testGetErrors() {
        inputDataSet.setOtherAction(OtherEntityAction.list);
        inputDataSet.setFilterType("");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(fields);
        source = new CompanySource(inputDataSet, i18n, authorizationClient, companyClient);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] filterType not specified", e.getMessage());
        }
    }

    @Test
    void testInvalidAccessToken() {
        inputDataSet.getDataStore().setEndpoint(MARKETO_ENDPOINT + "/bzh");
        source = new CompanySource(inputDataSet, i18n, authorizationClient, companyClient);
        try {
            source.init();
            fail("Should have a 403 error. Should not be here");
        } catch (RuntimeException e) {
            System.err.println("[testInvalidAccessToken] {}" + e);
        }
    }
}
