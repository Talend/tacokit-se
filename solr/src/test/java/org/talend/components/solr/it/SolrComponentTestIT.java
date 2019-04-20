package org.talend.components.solr.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.solr.SolrTestExtension;
import org.talend.components.solr.common.FilterCriteria;
import org.talend.components.solr.common.SolrDataStore;
import org.talend.components.solr.common.SolrDataset;
import org.talend.components.solr.output.SolrAction;
import org.talend.components.solr.output.SolrProcessorOutputConfiguration;
import org.talend.components.solr.service.Messages;
import org.talend.components.solr.service.SolrConnectorService;
import org.talend.components.solr.service.SolrConnectorUtils;
import org.talend.components.solr.service.TestMessages;
import org.talend.components.solr.source.SolrInputMapperConfiguration;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents("org.talend.components.solr")
@ExtendWith(SolrTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SolrComponentTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    private final static Messages messages = new TestMessages();

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private SolrConnectorService solrConnectorService;

    private SolrTestExtension.TestContext testContext;

    @BeforeAll
    private void init(SolrTestExtension.TestContext testContext) {
        this.testContext = testContext;
    }

    private SolrInputMapperConfiguration createInputMapperConfiguration() {
        SolrInputMapperConfiguration inputMapperConfiguration = new SolrInputMapperConfiguration();
        inputMapperConfiguration.setDataset(testContext.getSolrConnection());
        return inputMapperConfiguration;
    }

    private SolrProcessorOutputConfiguration createSolrProcessorOutputConfiguration() {
        SolrProcessorOutputConfiguration solrProcessorOutputConfiguration = new SolrProcessorOutputConfiguration();
        solrProcessorOutputConfiguration.setDataset(testContext.getSolrConnection());
        return solrProcessorOutputConfiguration;
    }

    @Test
    @DisplayName("Solr")
    void inputTest() {
        SolrInputMapperConfiguration inputMapperConfiguration = createInputMapperConfiguration();
        inputMapperConfiguration.setRows("100");
        final String config = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();
        Job.components().component("SolrInput", "Solr://Input?" + config).component("collector", "test://collector").connections()
                .from("SolrInput").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertTrue(res.stream().map(e -> e.getString("id")).collect(Collectors.toSet()).containsAll(Arrays.asList("apple",
                "corsair", "samsung", "viewsonic", "ati", "belkin", "maxtor", "asus", "adata", "canon", "dell")));
    }

    @Test
    @DisplayName("Solr")
    void inputRawTest() {
        SolrInputMapperConfiguration inputMapperConfiguration = createInputMapperConfiguration();
        inputMapperConfiguration.setRows("100");
        inputMapperConfiguration.setRawQuery("q=id:adata");
        final String config = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();
        Job.ExecutorBuilder executorBuilder = Job.components().component("SolrInput", "Solr://Input?" + config)
                .component("collector", "test://collector").connections().from("SolrInput").to("collector").build();
        executorBuilder.run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertFalse(res.stream().map(e -> e.getString("id")).collect(Collectors.toSet()).contains("dell"));
        assertTrue(res.stream().map(e -> e.getString("id")).collect(Collectors.toSet()).contains("adata"));
    }

    @Test
    @DisplayName("UpdateTest")
    void outputUpdateTest() throws IOException, SolrServerException {
        SolrProcessorOutputConfiguration solrProcessorOutputConfiguration = createSolrProcessorOutputConfiguration();
        solrProcessorOutputConfiguration.setAction(SolrAction.UPSERT);
        final String config = configurationByExample().forInstance(solrProcessorOutputConfiguration).configured().toQueryString();

        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("address_s", "comp1").build(),
                factory.createObjectBuilder().add("address_s", "comp2").build(),
                factory.createObjectBuilder().add("address_s", "comp3").build(),
                factory.createObjectBuilder().add("address_s", "comp4").build()));

        Job.components().component("emitter", "test://emitter").component("SolrOutput", "Solr://Output?" + config).connections()
                .from("emitter").to("SolrOutput").build().run();

        List<SolrDocument> res = getFirs100Documents();
        assertTrue(res.stream().map(e -> e.get("address_s")).collect(Collectors.toSet())
                .containsAll(Arrays.asList("comp1", "comp2", "comp3")));
    }

    @Test
    @DisplayName("Solr")
    void outputDeleteTest() throws IOException, SolrServerException {
        SolrProcessorOutputConfiguration solrProcessorOutputConfiguration = createSolrProcessorOutputConfiguration();
        solrProcessorOutputConfiguration.setAction(SolrAction.DELETE);
        final String config = configurationByExample().forInstance(solrProcessorOutputConfiguration).configured().toQueryString();
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("id", "apple").build(),
                factory.createObjectBuilder().add("id", "corsair").build(),
                factory.createObjectBuilder().add("id", "samsung").build()));

        Job.components().component("emitter", "test://emitter").component("SolrOutput", "Solr://Output?" + config).connections()
                .from("emitter").to("SolrOutput").build().run();

        List<SolrDocument> res = getFirs100Documents();
        assertFalse(res.stream().map(e -> e.get("id")).collect(Collectors.toSet())
                .containsAll(Arrays.asList("apple", "corsair", "samsung")));
        assertTrue(res.stream().map(e -> e.get("id")).collect(Collectors.toSet())
                .containsAll(Arrays.asList("viewsonic", "ati", "belkin")));
    }

    private List<SolrDocument> getFirs100Documents() throws IOException, SolrServerException {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(
                testContext.getSolrConnection().getDataStore().getUrl() + SolrTestExtension.CORE).build();
        SolrQuery query = new SolrQuery("*:*");
        query.setRows(100);
        QueryRequest req = new QueryRequest(query);
        req.setBasicAuthCredentials(SolrTestExtension.LOGIN, SolrTestExtension.PASSWORD);
        return req.process(solrClient).getResults();
    }

    @Test
    @DisplayName("Guess schema")
    void guessTableSchemaTest() {
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        SolrConnectorUtils util = new SolrConnectorUtils();
        config.setDataset(testContext.getSolrConnection());
        Schema schema = solrConnectorService.guessTableSchema(config.getDataset(), util);

        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder().withName("id").withType(Schema.Type.STRING).build());
        Schema expectedSchema = schemaBuilder.build();

        assertEquals(expectedSchema, schema);
    }

    @Test
    @DisplayName("Guess schema failed test")
    void guessSchemaFailedTest() {
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        SolrConnectorUtils util = new SolrConnectorUtils();
        SolrDataset connection = new SolrDataset();
        connection.setCore(SolrTestExtension.CORE);
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl("https://localhost:8983/badsolrurl");
        dataStore.setLogin(SolrTestExtension.LOGIN);
        dataStore.setPassword(SolrTestExtension.PASSWORD);
        connection.setDataStore(dataStore);
        config.setDataset(connection);
        Schema schema = solrConnectorService.guessTableSchema(config.getDataset(), util);
        assertTrue(schema.getEntries().isEmpty());
    }

    @Test
    @DisplayName("Check Connection")
    void checkConnectionTest() {
        SolrConnectorUtils util = new SolrConnectorUtils();
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl(testContext.getSolrConnection().getDataStore().getUrl());
        dataStore.setLogin(SolrTestExtension.LOGIN);
        dataStore.setPassword(SolrTestExtension.PASSWORD);
        HealthCheckStatus status = solrConnectorService.checkConnection(dataStore, messages, util);
        assertEquals("OK", status.getStatus().name());
    }

    @Test
    @DisplayName("Check Failed Connection")
    void checkConnectionNegativeTest() {
        SolrConnectorUtils util = new SolrConnectorUtils();
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl("http://localhost:8982/badsolrurl");
        dataStore.setLogin(SolrTestExtension.LOGIN);
        dataStore.setPassword(SolrTestExtension.PASSWORD);
        HealthCheckStatus status = solrConnectorService.checkConnection(dataStore, messages, util);
        assertEquals("KO", status.getStatus().name());
    }

    @Test
    @DisplayName("Check suggestCore")
    void suggestTest() {
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        SolrConnectorUtils util = new SolrConnectorUtils();
        config.setDataset(testContext.getSolrConnection());
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl(testContext.getSolrConnection().getDataStore().getUrl());
        dataStore.setPassword(SolrTestExtension.PASSWORD);
        dataStore.setLogin(SolrTestExtension.LOGIN);
        SuggestionValues values = solrConnectorService.suggestCore(dataStore, util);
        assertEquals(Arrays.asList(new SuggestionValues.Item(SolrTestExtension.CORE, SolrTestExtension.CORE)), values.getItems());
    }

    @Test
    @DisplayName("Check suggestCore")
    void testSuggestRawQuery() {
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        config.setDataset(testContext.getSolrConnection());
        FilterCriteria criteriaId = new FilterCriteria();
        criteriaId.setField("id");
        criteriaId.setValue("apple");
        FilterCriteria criteriaComp = new FilterCriteria();
        criteriaComp.setField("compName_s");
        criteriaComp.setValue("Apple");
        config.setFilterQuery(Arrays.asList(criteriaId, criteriaComp));
        config.setRows("1");
        config.setStart("1");
        SuggestionValues values = solrConnectorService.suggestRawQuery(config, new SolrConnectorUtils());
        SuggestionValues expected = new SuggestionValues();
        SuggestionValues.Item item = new SuggestionValues.Item();
        item.setId("q=*:*&fq=id:apple&fq=compName_s:Apple&rows=1&start=1");
        item.setLabel("q=*:*&fq=id:apple&fq=compName_s:Apple&rows=1&start=1");
        expected.setItems(Arrays.asList(item));
        assertEquals(expected, values);
    }

}
