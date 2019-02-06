package org.talend.components.jms.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.jms.testutils.JmsTestConstants.JMS_PROVIDER;

@WithComponents("org.talend.components.jms")
class ActionServiceTest {

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
        dataStore.setUrl("wrong_url");
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertEquals("Invalid connection", status.getComment());
    }

    @Test
    public void testGuessSchema() {
        Schema schema = actionService.guessSchema(new BasicConfiguration());
        Assertions.assertNotNull(schema, "Guess Schema should not be null");
        Optional<Schema.Entry> optional = schema.getEntries().stream().findFirst();
        Assertions.assertTrue(optional.isPresent(), "Guess Schema Entry was not set");
        Assertions.assertEquals(MESSAGE_CONTENT, optional.get().getName());
        Assertions.assertEquals(Schema.Type.STRING, optional.get().getType());
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
