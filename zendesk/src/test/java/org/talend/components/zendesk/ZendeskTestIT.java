package org.talend.components.zendesk;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.components.zendesk.helpers.JsonHelper;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.components.zendesk.sources.delete.ZendeskDeleteConfiguration;
import org.talend.components.zendesk.sources.get.InputIterator;
import org.talend.components.zendesk.sources.get.ZendeskGetConfiguration;
import org.talend.components.zendesk.sources.put.ZendeskPutConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.zendesk.client.v2.model.Ticket;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@DisplayName("Suite of test for the Zendesk components")
@WithComponents("org.talend.components.zendesk")
@ExtendWith(ZendeskTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZendeskTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private ZendeskHttpClientService zendeskHttpClientService;

    private ZendeskTestExtension.TestContext testContext;

    private static final String TEST_DATA_COLLECTOR = "test://collector";

    private static final String TEST_DATA_EMITTER = "test://emitter";

    @BeforeAll
    private void init(ZendeskTestExtension.TestContext testContext) {
        log.info("init service test");
        this.testContext = testContext;
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Input. Get ticket by id")
    void inputComponentGetTicket(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Input. Get ticket by id'. Data store: " + dataStoreCustom);

        JsonObject ticket = getFirstTicket(dataStoreCustom);
        int firstTicketId = ticket.getInt("id");

        ZendeskGetConfiguration zendeskGetConfiguration = new ZendeskGetConfiguration();
        zendeskGetConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStoreCustom));
        zendeskGetConfiguration.setQueryString(Integer.toString(firstTicketId));
        final String config = SimpleFactory.configurationByExample().forInstance(zendeskGetConfiguration).configured()
                .toQueryString();
        Job.components().component("zendesk-input", "Zendesk://Input?" + config).component("collector", TEST_DATA_COLLECTOR)
                .connections().from("zendesk-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(1, res.get(0).getInt("id"));
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Update custom ticket")
    void outputComponentUpdate(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Output. Update custom ticket'. Data store: " + dataStoreCustom);

        JsonObject ticket = getFirstTicket(dataStoreCustom);
        int firstTicketId = ticket.getInt("id");

        ZendeskPutConfiguration zendeskPutConfiguration = new ZendeskPutConfiguration();
        zendeskPutConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStoreCustom));
        final String config = SimpleFactory.configurationByExample().forInstance(zendeskPutConfiguration).configured()
                .toQueryString();

        String newSubject = "Updated subject. " + new Date();
        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("id", firstTicketId).add("subject", newSubject)
                .build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", TEST_DATA_EMITTER).component("zendesk-output", "Zendesk://Output?" + config)
                .component("collector", TEST_DATA_COLLECTOR).connections().from("emitter").to("zendesk-output")
                .from("zendesk-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(newSubject, res.get(0).getString("subject"));
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Update custom ticket (batch)")
    void outputComponentUpdateBatch(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Output. Update custom ticket (batch)'. Data store: " + dataStoreCustom);

        JsonObject ticket = getFirstTicket(dataStoreCustom);
        int firstTicketId = ticket.getInt("id");

        ZendeskPutConfiguration zendeskPutConfiguration = new ZendeskPutConfiguration();
        zendeskPutConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStoreCustom));
        zendeskPutConfiguration.setUseBatch(true);
        final String config = SimpleFactory.configurationByExample().forInstance(zendeskPutConfiguration).configured()
                .toQueryString();

        String newSubject = "Updated subject. " + new Date();
        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("id", firstTicketId).add("subject", newSubject)
                .build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", TEST_DATA_EMITTER).component("zendesk-output", "Zendesk://Output?" + config)
                .component("collector", TEST_DATA_COLLECTOR).connections().from("emitter").to("zendesk-output")
                .from("zendesk-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());

        JsonObject updatedTicket = getTicketById(dataStoreCustom, res.get(0).getInt("id"));
        Assertions.assertEquals(newSubject, updatedTicket.getString("subject"));
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Write custom ticket")
    void outputComponentCreate(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Output. Write custom ticket'. Data store: " + dataStoreCustom);

        JsonObject ticket = getFirstTicket(dataStoreCustom);
        int firstTicketId = ticket.getInt("id");

        ZendeskPutConfiguration zendeskPutConfiguration = new ZendeskPutConfiguration();
        zendeskPutConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStoreCustom));
        final String config = SimpleFactory.configurationByExample().forInstance(zendeskPutConfiguration).configured()
                .toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder(ticket).remove("id").remove("forum_topic_id").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", TEST_DATA_EMITTER).component("zendesk-output", "Zendesk://Output?" + config)
                .component("collector", TEST_DATA_COLLECTOR).connections().from("emitter").to("zendesk-output")
                .from("zendesk-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertNotNull(res.get(0).getInt("id"));
        Assertions.assertNotEquals(firstTicketId, res.get(0).getInt("id"));
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Create custom ticket (batch)")
    void outputComponentCreateBatch(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Output. Write custom ticket'. Data store: " + dataStoreCustom);

        JsonObject ticket = getFirstTicket(dataStoreCustom);
        int firstTicketId = ticket.getInt("id");

        ZendeskPutConfiguration zendeskPutConfiguration = new ZendeskPutConfiguration();
        zendeskPutConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStoreCustom));
        zendeskPutConfiguration.setUseBatch(true);
        final String config = SimpleFactory.configurationByExample().forInstance(zendeskPutConfiguration).configured()
                .toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder(ticket).remove("id").remove("forum_topic_id").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", TEST_DATA_EMITTER).component("zendesk-output", "Zendesk://Output?" + config)
                .component("collector", TEST_DATA_COLLECTOR).connections().from("emitter").to("zendesk-output")
                .from("zendesk-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertNotNull(res.get(0).getInt("id"));
        Assertions.assertNotEquals(firstTicketId, res.get(0).getInt("id"));
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Delete. Delete new ticket")
    void deleteComponent(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Delete. Delete new ticket'. Data store: " + dataStoreCustom);

        JsonObject ticket = createTicketDirectly(dataStoreCustom);
        Integer newTicketId = ticket.getInt("id");
        Assertions.assertNotNull(newTicketId);

        ZendeskDeleteConfiguration zendeskDeleteConfiguration = new ZendeskDeleteConfiguration();
        zendeskDeleteConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStoreCustom));
        final String config = SimpleFactory.configurationByExample().forInstance(zendeskDeleteConfiguration).configured()
                .toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("id", newTicketId).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", TEST_DATA_EMITTER).component("zendesk-delete", "Zendesk://Delete?" + config)
                .component("collector", TEST_DATA_COLLECTOR).connections().from("emitter").to("zendesk-delete")
                .from("zendesk-delete").to("collector").build().run();
        // wait a little bit (the deleted ticket can be seen via id some period of time after deleting)
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        JsonObject oldTicket = getTicketById(dataStoreCustom, newTicketId);
        Assertions.assertNull(oldTicket);
    }

    private Stream<Arguments> methodSourceDataStores() {
        return testContext.getDataStores();
    }

    private JsonObject getFirstTicket(ZendeskDataStore dataStore) {
        ZendeskGetConfiguration zendeskGetConfiguration = new ZendeskGetConfiguration();
        zendeskGetConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStore));

        InputIterator ticketIterator = zendeskHttpClientService.getTickets(zendeskGetConfiguration);
        if (!ticketIterator.hasNext()) {
            throw new RuntimeException("No tickets found");
        }
        return ticketIterator.next();
    }

    private JsonObject getTicketById(ZendeskDataStore dataStore, Integer ticketId) {
        ZendeskGetConfiguration zendeskGetConfiguration = new ZendeskGetConfiguration();
        zendeskGetConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStore));
        zendeskGetConfiguration.setQueryString(ticketId.toString());

        InputIterator ticketIterator = zendeskHttpClientService.getTickets(zendeskGetConfiguration);
        if (!ticketIterator.hasNext()) {
            return null;
        }
        return ticketIterator.next();
    }

    private JsonObject createTicketDirectly(ZendeskDataStore dataStore) {
        JsonObject ticketJson = getFirstTicket(dataStore);
        Ticket ticket = JsonHelper.toInstance(ticketJson, Ticket.class);
        ticket.setId(null);
        ticket.setForumTopicId(null);

        ZendeskGetConfiguration zendeskGetConfiguration = new ZendeskGetConfiguration();
        zendeskGetConfiguration.setDataSet(ZendeskTestHelper.getTicketDataSet(dataStore));

        JsonObject newTicket = zendeskHttpClientService.putTicket(dataStore, ticket);
        if (newTicket == null) {
            throw new RuntimeException("Ticket creating error");
        }
        return newTicket;
    }

}
