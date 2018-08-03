package org.talend.components.jms.service;

import org.junit.jupiter.api.Test;
import org.talend.components.jms.configuration.MessageType;
import org.talend.components.jms.output.OutputConfiguration;
import org.talend.components.jms.source.DurableSubscriptionConfiguration;
import org.talend.components.jms.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.Assert.assertEquals;

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

    @Test
    void sendAndReceiveJMSMessageQueue() {
        jmsService.setConfiguration(getOutputConfiguration());
        jmsService.sendTextMessage(TEST_MESSAGE);

        jmsService.setConfiguration(getInputConfiguration());
        assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
    }

    @Test
    void sendJMSMessageTopic() {
        OutputConfiguration configuration = new OutputConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setDestination(DESTINATION);
        configuration.setMessageType(MessageType.TOPIC);
        jmsService.setConfiguration(configuration);
        jmsService.sendTextMessage(TEST_MESSAGE);

        //assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
    }

    @Test
    void receiveJMSMessageTopic() {
        InputMapperConfiguration configuration = getInputConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setDestination(DESTINATION);
        configuration.setMessageType(MessageType.TOPIC);
        configuration.getSubscriptionConfig().setDurableSubscription(true);
        configuration.setTimeout(0);

        jmsService.setConfiguration(configuration);
        // jmsService.sendTextMessage(TEST_MESSAGE);

        System.out.println(jmsService.receiveTextMessage());;
//        assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
    }


    private OutputConfiguration getOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setDestination(DESTINATION);
        configuration.setMessageType(MessageType.QUEUE);
        return configuration;
    }

    private InputMapperConfiguration getInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setDestination(DESTINATION);
        configuration.setMessageType(MessageType.QUEUE);
        DurableSubscriptionConfiguration subsConfig = new DurableSubscriptionConfiguration();
        subsConfig.setDurableSubscription(DURABLE_SUBSCRIPTION);
        subsConfig.setClientId(CLIENT_ID);
        subsConfig.setSubscriberName(SUBSCRIBER_NAME);
        configuration.setSubscriptionConfig(subsConfig);
        configuration.setTimeout(TIMEOUT);
        return configuration;
    }

     @Test
     void sendJMSMessageWithUserIdentity() {
     InputMapperConfiguration configuration = new InputMapperConfiguration();
     configuration.setModuleList(ACTIVEMQ);
     configuration.setUserIdentity(true);
     configuration.setUrl(URL);
     configuration.setDestination(DESTINATION);
     configuration.setMessageType(MessageType.QUEUE);
     jmsService.setConfiguration(configuration);
     jmsService.sendTextMessage(TEST_MESSAGE);

     assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
     }
}
