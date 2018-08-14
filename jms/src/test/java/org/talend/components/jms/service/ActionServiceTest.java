package org.talend.components.jms.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.components.jms.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;
import org.talend.sdk.component.junit5.WithComponents;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;

@WithComponents("org.talend.components.jms")
class ActionServiceTest {

    public static String JMS_PROVIDER = "ACTIVEMQ";

    public static final String URL = "tcp://localhost:61616";

    @Service
    private ActionService actionService;

    @Test
    @DisplayName("DynamicValue - Load Providers")
    public void loadSupportedDataBaseTypes() {
        final Values values = actionService.loadSupportedJMSProviders();
        assertNotNull(values);
        assertEquals(1, values.getItems().size());

        assertEquals(asList(JMS_PROVIDER), values.getItems().stream().map(Values.Item::getId).collect(toList()));
    }

    @Test
    public void testJMSNoConnection() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(JMS_PROVIDER);
        dataStore.setUrl(URL);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertEquals("Invalid connection", status.getComment());
    }

    @Test
    public void testGuessSchema() {
        assertTrue(actionService.guessSchema(new InputMapperConfiguration()).getEntries()
                .contains(new Schema.Entry(MESSAGE_CONTENT, Type.STRING)));
    }

    @Test
    public void testJMSConnectionEmptyUrl() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(JMS_PROVIDER);
        dataStore.setUrl("");
        assertThrows(IllegalArgumentException.class, () -> {
            actionService.validateBasicDatastore(dataStore);
        });
    }

}
