package org.talend.components.jms.service;

import org.junit.jupiter.api.Test;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.configuration.MessageType;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.components.jms.output.OutputConfiguration;
import org.talend.components.jms.source.DurableSubscriptionConfiguration;
import org.talend.components.jms.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonObject;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.jms") // component package
public class JMSServiceTest {

    public static final String URL = "tcp://localhost:61616";

    public static final String DESTINATION = "test";

    public static final String TEST_MESSAGE = "hello world";

    public static final String ACTIVEMQ = "ACTIVEMQ";

    public static final boolean USER_IDENTITY = true;

    public static final boolean DURABLE_SUBSCRIPTION = false;

    public static final String CLIENT_ID = "ClientId";

    public static final String SUBSCRIBER_NAME = "subscrName";

    public static final int TIMEOUT = 1;

    @Service
    private JmsService jmsService;

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    void sendAndReceiveJMSMessageQueue() {
        // jmsService.setBasicConfig(getOutputConfiguration());
        // jmsService.sendTextMessage(TEST_MESSAGE);
        //
        // jmsService.setBasicConfig(getInputConfiguration());
        // assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
        OutputConfiguration outputConfiguration = getOutputConfiguration();
        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("jms-output", "JMS://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("jms-output").build().run();

        // InputMapperConfiguration inputConfiguration = getInputConfiguration();
        // final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        // Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector",
        // "test://collector").connections()
        // .from("jms-input").to("collector").build().run();
        //
        // final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        //
        // System.out.println("test results" + res);
    }

    @Test
    void receiveJMSMessageQueue() {
        // jmsService.setBasicConfig(getOutputConfiguration());
        // jmsService.sendTextMessage(TEST_MESSAGE);
        //
        // jmsService.setBasicConfig(getInputConfiguration());
        // assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
        // OutputConfiguration outputConfiguration = getOutputConfiguration();
        // final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        // Job.components().component("jms-output", "JMS://Output?" + outputConfig).component("sender",
        // "test://sender").connections()
        // .from("sender").to("jms-output").build().run();

        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("jms-input", "JMS://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("jms-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);

        System.out.println("test results" + res);
    }

    /*
     * * * @Test
     * void sendJMSMessageTopic() {
     * OutputConfiguration basicConfig = new OutputConfiguration();
     * basicConfig.setModuleList(ACTIVEMQ);
     * basicConfig.setUrl(URL);
     * basicConfig.setDestination(DESTINATION);
     * basicConfig.setMessageType(MessageType.TOPIC);
     * jmsService.setBasicConfig(basicConfig);
     * jmsService.sendTextMessage(TEST_MESSAGE);
     *
     * // assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
     * }
     *
     * @Test
     * void receiveJMSMessageTopic() {
     * InputMapperConfiguration basicConfig = getInputConfiguration();
     * basicConfig.setModuleList(ACTIVEMQ);
     * basicConfig.setUrl(URL);
     * basicConfig.setDestination(DESTINATION);
     * basicConfig.setMessageType(MessageType.TOPIC);
     * basicConfig.getSubscriptionConfig().setDurableSubscription(true);
     * basicConfig.setTimeout(0);
     *
     * jmsService.setBasicConfig(basicConfig);
     * // jmsService.sendTextMessage(TEST_MESSAGE);
     *
     * System.out.println(jmsService.receiveTextMessage());
     * ;
     * // assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
     * }
     */

    private OutputConfiguration getOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(ACTIVEMQ);
        dataStore.setUrl(URL);
        dataStore.setUserName("admin");
        dataStore.setPassword("anything");
        dataStore.setUserIdentity(true);
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        configuration.setConnection(dataStore);
        configuration.setBasicConfig(basicConfiguration);
        return configuration;
    }

    private InputMapperConfiguration getInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(ACTIVEMQ);
        dataStore.setUrl(URL);
        dataStore.setUserName("admin");
        dataStore.setPassword("anything");
        dataStore.setUserIdentity(true);
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        DurableSubscriptionConfiguration subsConfig = new DurableSubscriptionConfiguration();
        subsConfig.setDurableSubscription(DURABLE_SUBSCRIPTION);
        subsConfig.setClientId(CLIENT_ID);
        subsConfig.setSubscriberName(SUBSCRIBER_NAME);
        configuration.setConnection(dataStore);
        configuration.setSubscriptionConfig(subsConfig);
        configuration.setBasicConfig(basicConfiguration);
        configuration.setMaximumMessages(10);
        configuration.setTimeout(TIMEOUT);
        return configuration;
    }
    /*
     * @Test
     * void sendJMSMessageWithUserIdentity() {
     * OutputConfiguration basicConfig = new OutputConfiguration();
     * basicConfig.setModuleList(ACTIVEMQ);
     * // basicConfig.setUserIdentity(true);
     * basicConfig.setUrl(URL);
     * basicConfig.setDestination(DESTINATION);
     * basicConfig.setMessageType(MessageType.QUEUE);
     * basicConfig.setUserName("admin");
     * basicConfig.setPassword("anything");
     * jmsService.setBasicConfig(basicConfig);
     * jmsService.sendTextMessage(TEST_MESSAGE);
     *
     * // assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
     * }
     */

}
