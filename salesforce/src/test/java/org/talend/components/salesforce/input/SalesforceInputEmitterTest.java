package org.talend.components.salesforce.input;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.salesforce.SalesforceTestBase;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.dataset.SOQLQueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.output.OutputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@Disabled("Salesforce credentials is not ready on ci")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@WithComponents("org.talend.components.salesforce")
public class SalesforceInputEmitterTest extends SalesforceTestBase {

    private static final String UNIQUE_ID;

    static {
        UNIQUE_ID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));
    }

    @Service
    private RecordBuilderFactory factory;

    @BeforeAll
    public void testprepareData() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);

        RecordBuilderFactory factory = getComponentsHandler().findService(RecordBuilderFactory.class);

        List<Record> records = new ArrayList<>(10);
        for (int i = 0; i < 300; i++) {
            Record.Builder recordBuilder = factory.newRecordBuilder();
            recordBuilder.withString("Name", "TestName_" + i + "_" + UNIQUE_ID);
            if (i % 40 == 0) {
                recordBuilder.withInt("NumberOfEmployees", 8000);
            }
            if (i % 80 == 0) {
                factory.newRecordBuilder().withDouble("AnnualRevenue", 5000.0);
            }
            records.add(recordBuilder.build());
        }

        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + config).connections().from("emitter")
                .to("salesforce-output").build().run();

    }

    // Module Query part
    @Test
    @DisplayName("Test query with module name")
    public void testModuleQuery() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id", "Name"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(getDataStore());
        moduleDataSet.setCondition("Name Like 'TestName_%" + UNIQUE_ID + "%'");

        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(300, records.size());
        Record record = records.get(0);
        Schema schema = record.getSchema();
        List<Schema.Entry> entries = schema.getEntries();
        assertEquals(2, entries.size());
    }

    @Test
    @DisplayName("Test query module record with limit")
    public void testModuleQueryWithLimit() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Name"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(getDataStore());
        moduleDataSet.setCondition("Name Like 'TestName_%" + UNIQUE_ID + "%' limit 10");

        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertEquals(10, records.size());

        records.stream().forEach(r -> assertTrue(records.iterator().next().getString("Name").contains("TestName")));
    }

    @Test
    @DisplayName("Test query module record with all types")
    public void testQueryAllType() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id", "IsDeleted", "MasterRecordId", "Name", "Type", "ParentId",
                "BillingStreet", "BillingCity", "BillingState", "BillingPostalCode", "BillingCountry", "BillingLatitude",
                "BillingLongitude", "ShippingStreet", "ShippingCity", "ShippingState", "ShippingPostalCode", "ShippingCountry",
                "ShippingLatitude", "ShippingLongitude", "Phone", "Fax", "AccountNumber", "Website", "PhotoUrl", "Sic",
                "Industry", "AnnualRevenue", "NumberOfEmployees", "Ownership", "TickerSymbol", "Description", "Rating", "Site",
                "OwnerId", "CreatedDate", "CreatedById", "LastModifiedDate", "LastModifiedById", "SystemModstamp",
                "LastActivityDate", "LastViewedDate", "LastReferencedDate", "Jigsaw", "JigsawCompanyId", "AccountSource",
                "SicDesc"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(getDataStore());
        moduleDataSet.setCondition("Name Like 'TestName_%" + UNIQUE_ID + "%' and NumberOfEmployees != null");

        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertEquals(8, records.size());
        Record record = records.get(0);
        assertEquals(8000, record.getInt("NumberOfEmployees"));
    }

    @Test
    @DisplayName("Test query with bad credential")
    public void testQueryWithBadCredential() {
        final BasicDataStore datstore = new BasicDataStore();
        datstore.setEndpoint(URL);
        datstore.setUserId("badUser");
        datstore.setPassword("badPasswd");
        datstore.setSecurityKey("badSecurityKey");
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("account");
        moduleDataSet.setDataStore(datstore);
        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @DisplayName("Test module query with invalid name")
    public void testModuleQueryWithInvalideName() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("invalid0");
        moduleDataSet.setDataStore(getDataStore());
        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @DisplayName("Test module query with invalid field")
    public void testModuleQueryWithInvalidField() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(singletonList("InvalidField10x"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(getDataStore());
        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    // SOQL Query part
    @Test
    @DisplayName("Test basic SOQL query")
    public void testSOQLQueryBasicCase() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("select Id,Name,IsDeleted from account where Name Like 'TestName_%" + UNIQUE_ID + "%'");
        soqlQueryDataSet.setDataStore(getDataStore());

        final String config = configurationByExample().forInstance(soqlQueryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://SOQLQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertEquals(300, records.size());
        Record record = records.get(0);
        Schema schema = record.getSchema();
        assertNotNull(schema);
        assertEquals(3, schema.getEntries().size());
        assertTrue(record.getString("Name").contains("TestName"));
    }

    @Test
    @DisplayName("Test SOQL query return empty result")
    public void testSOQLQueryWithEmptyResult() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("select  name from account where name = 'this name will never exist $'");
        soqlQueryDataSet.setDataStore(getDataStore());

        final String config = configurationByExample().forInstance(soqlQueryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://SOQLQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertEquals(0, records.size());
    }

    @Test
    @DisplayName("Test SOQL query with Invalid query")
    public void testSOQLQueryWithInvalidQuery() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("from account");
        soqlQueryDataSet.setDataStore(getDataStore());
        final String config = configurationByExample().forInstance(soqlQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://SOQLQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @DisplayName("Test SOQL query with relationship")
    public void testSOQLQueryChildToParent() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("select Id,Name,CreatedBy.Name from Account where Name Like 'TestName_100%" + UNIQUE_ID + "%'");
        soqlQueryDataSet.setDataStore(getDataStore());

        final String config = configurationByExample().forInstance(soqlQueryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://SOQLQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertEquals(1, records.size());
        Record record = records.get(0);
        assertNotNull(record.getString("CreatedBy_Name"));
    }

    @AfterAll
    public void test99_cleanupTestRecords() {
        cleanTestRecords("Account", "Name Like '%" + UNIQUE_ID + "%'");
        checkModuleData("Account", "Name Like '%" + UNIQUE_ID + "%'", 0);
    }

}