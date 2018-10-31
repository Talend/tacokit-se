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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import javax.json.JsonObject;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.chain.Job;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
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
        source = new CompanySource(inputDataSet, service, tools);
        source.init();
        result = source.next();
        assertNotNull(result);
        assertEquals(fields, result.getString(ATTR_FIELDS));
        // assertEquals(fields, service.getFieldsFromDescribeFormatedForApi(result.getJsonArray(ATTR_FIELDS)));
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
        source = new CompanySource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            Assert.assertThat(result.getString("externalCompanyId"), CoreMatchers.containsString("google0"));
        }
    }

    @Test
    void testGetCompaniesFails() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("billingCountry");
        inputDataSet.setFilterValues("France");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
        source = new CompanySource(inputDataSet, service, tools);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] Invalid filterType 'billingCountry'", e.getMessage());
        }
    }

    @Test
    public void testDescribeCompaniesWithCreateMapper() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        final Mapper mapper = component.createMapper(MarketoInputMapper.class, inputDataSet);
        List<JsonObject> res = component.collectAsList(JsonObject.class, mapper);
        LOG.warn("[testDescribeCompaniesWithCreateMapper] {}", res.get(0));
        assertEquals(1, res.size());
        JsonObject record2 = res.get(0).asJsonObject();
    }

    @Test
    void testGetCompaniesWithCreateMapper() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(fields);
        final Mapper mapper = component.createMapper(MarketoInputMapper.class, inputDataSet);
        List<JsonObject> res = component.collectAsList(JsonObject.class, mapper);
        assertEquals(4, res.size());
        JsonObject record = res.get(0).asJsonObject();
        assertThat(record.getString("externalCompanyId"), CoreMatchers.containsString("google0"));
        assertEquals(JSON_VALUE_XUNDEFINED_X, record.getString("industry", JSON_VALUE_XUNDEFINED_X));
    }

    @Test
    void testGetErrors() {
        inputDataSet.setOtherAction(OtherEntityAction.list);
        inputDataSet.setFilterType("");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(fields);
        source = new CompanySource(inputDataSet, service, tools);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] filterType not specified", e.getMessage());
        }
    }

    @Test
    void testInvalidAccessToken() {
        inputDataSet.getDataStore().setEndpoint(MARKETO_ENDPOINT + "/bzh");
        source = new CompanySource(inputDataSet, service, tools);
        try {
            source.init();
            fail("Should have a 403 error. Should not be here");
        } catch (RuntimeException e) {
            System.err.println("[testInvalidAccessToken] {}" + e);
        }
    }

    @Test
    void getCompanies() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
        final String config = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        LOG.warn("[getCompanies] config: {}", config);
        Job.components().component("MktoInput", "Marketo://Input?" + config).component("collector", "test://collector")
                .connections().from("MktoInput").to("collector").build().run();
        final List<JsonObject> records = component.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertEquals(4, records.size());
    }

    @Test
    @DisplayName("Beam Test getCompanies")
    public void produce() {
        Record r1 = service.getRecordBuilder().newRecordBuilder().withInt("seq", 0).withInt("id", 53)
                .withString("website", "g.plus").withString("externalCompanyId", "google02")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();
        Record r2 = service.getRecordBuilder().newRecordBuilder().withInt("seq", 1).withInt("id", 54)
                .withString("website", "g.plus").withString("externalCompanyId", "google03")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();
        Record r3 = service.getRecordBuilder().newRecordBuilder().withFloat("seq", 2).withFloat("id", 55)
                .withString("website", "g.plus").withString("externalCompanyId", "google04")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();
        Record r4 = service.getRecordBuilder().newRecordBuilder().withFloat("seq", 3).withFloat("id", 56)
                .withString("website", "g.plus").withString("externalCompanyId", "google05")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();

        // Setup your component configuration for the test here
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
        //
        Pipeline pipeline = Pipeline.create();
        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = component.createMapper(MarketoInputMapper.class, inputDataSet);
        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));
        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        // PAssert.that(out).containsInAnyOrder(r1);
        // finally run the pipeline and ensure it was successful - i.e. data were validated
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());

    }
}
