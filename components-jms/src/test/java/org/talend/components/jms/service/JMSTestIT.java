package org.talend.components.jms.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.configuration.MessageType;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.components.jms.output.OutputConfiguration;
import org.talend.components.jms.source.DurableSubscriptionConfiguration;
import org.talend.components.jms.source.InputMapperConfiguration;
import org.talend.components.jms.testutils.JMSTestExtention;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.jms.testutils.JmsTestConstants.CLIENT_ID;
import static org.talend.components.jms.testutils.JmsTestConstants.DESTINATION;
import static org.talend.components.jms.testutils.JmsTestConstants.DURABLE_SUBSCRIPTION;
import static org.talend.components.jms.testutils.JmsTestConstants.JMS_PROVIDER;
import static org.talend.components.jms.testutils.JmsTestConstants.MISSING_PROVIDER;
import static org.talend.components.jms.testutils.JmsTestConstants.NO_MESSAGES;
import static org.talend.components.jms.testutils.JmsTestConstants.SUBSCRIBER_NAME;
import static org.talend.components.jms.testutils.JmsTestConstants.TEN_MESSAGES;
import static org.talend.components.jms.testutils.JmsTestConstants.TEST_MESSAGE;
import static org.talend.components.jms.testutils.JmsTestConstants.TEST_MESSAGE2;
import static org.talend.components.jms.testutils.JmsTestConstants.TIMEOUT;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.jms") // component package
@ExtendWith(JMSTestExtention.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JMSTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    private JMSTestExtention.TestContext testContext;

    @BeforeAll
    private void init(JMSTestExtention.TestContext testContext) {
        this.testContext = testContext;
    }

    @Test
    public void sendAndReceiveJMSMessageQueue() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("messageContent", TEST_MESSAGE).build()));

        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("jms-output", "JMS://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("jms-output").build().run();

        // Receive message from QUEUE
        final String inputConfig = configurationByExample().forInstance(getInputConfiguration()).configured().toQueryString();

        Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopic() {
        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("messageContent", TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);
        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("jms-output", "JMS://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("jms-output").build().run();

        // Receive message from TOPIC
        Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void missingProviderConfig() {
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setModuleList(MISSING_PROVIDER);
        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        assertThrows(IllegalStateException.class, () -> Job.components().component("jms-input", "JMS://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("jms-input").to("collector").build().run());
    }

    @Test
    public void emptyUrlTest() {
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setUrl("");
        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jms-input", "JMS://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("jms-input").to("collector").build().run());
    }

    @Test
    public void testMessageSelector() {
        // Send Not Persistent message
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("messageContent", TEST_MESSAGE).build()));
        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("jms-output", "JMS://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("jms-output").build().run();

        // Send Persistent message
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("messageContent", TEST_MESSAGE2).build()));
        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.setDeliveryMode(OutputConfiguration.DeliveryMode.PERSISTENT);
        final String outputConfig2 = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("jms-output", "JMS://Output?" + outputConfig2).component("emitter", "test://emitter")
                .connections().from("emitter").to("jms-output").build().run();

        // Add filter query to receive only PERSISTENT message
        InputMapperConfiguration inputMapperConfiguration = getInputConfiguration();
        inputMapperConfiguration.setMessageSelector("JMSDeliveryMode = 'PERSISTENT'");
        final String inputConfig = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();

        Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE2, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");

    }

    @Test
    public void testMaximumMessages() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("messageContent", TEST_MESSAGE).build()));

        final String outputConfig2 = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("jms-output", "JMS://Output?" + outputConfig2).component("emitter", "test://emitter")
                .connections().from("emitter").to("jms-output").build().run();

        // Forbid message receiving
        InputMapperConfiguration inputMapperConfiguration = getInputConfiguration();
        inputMapperConfiguration.setMaximumMessages(NO_MESSAGES);
        final String inputConfig = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();

        Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertTrue(res.isEmpty(), "Component should not process data");

        // Allow message receiving
        InputMapperConfiguration inputMapperConfiguration2 = getInputConfiguration();
        inputMapperConfiguration.setMaximumMessages(TEN_MESSAGES);
        final String inputConfig2 = configurationByExample().forInstance(inputMapperConfiguration2).configured().toQueryString();

        Job.components().component("jms-input", "JMS://Input?" + inputConfig2).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        final List<JsonObject> res2 = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res2.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");

    }

    private OutputConfiguration getOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(JMS_PROVIDER);
        dataStore.setUrl(testContext.getURL());
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        basicConfiguration.setConnection(dataStore);
        configuration.setBasicConfig(basicConfiguration);
        return configuration;
    }

    private InputMapperConfiguration getInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(JMS_PROVIDER);
        dataStore.setUrl(testContext.getURL());
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        DurableSubscriptionConfiguration subsConfig = new DurableSubscriptionConfiguration();
        subsConfig.setDurableSubscription(DURABLE_SUBSCRIPTION);
        subsConfig.setClientId(CLIENT_ID);
        subsConfig.setSubscriberName(SUBSCRIBER_NAME);
        basicConfiguration.setConnection(dataStore);
        configuration.setSubscriptionConfig(subsConfig);
        configuration.setBasicConfig(basicConfiguration);
        configuration.setMaximumMessages(TEN_MESSAGES);
        configuration.setTimeout(TIMEOUT);
        return configuration;
    }

}
