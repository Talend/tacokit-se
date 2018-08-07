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

    /*
     * public static final String URL = "tcp://localhost:61616";
     * 
     * public static final String DESTINATION = "test";
     * 
     * public static final String TEST_MESSAGE = "hello world";
     * 
     * public static final String ACTIVEMQ = "ACTIVEMQ";
     * 
     * public static final boolean USER_IDENTITY = true;
     * 
     * public static final boolean DURABLE_SUBSCRIPTION = false;
     * 
     * public static final String CLIENT_ID = "ClientId";
     * 
     * public static final String SUBSCRIBER_NAME = "subscrName";
     * 
     * public static final int TIMEOUT = 1;
     * 
     * @Service
     * private JmsService jmsService;
     * 
     * @Test
     * void sendAndReceiveJMSMessageQueue() {
     * jmsService.setBasicConfig(getOutputConfiguration());
     * jmsService.sendTextMessage(TEST_MESSAGE);
     * 
     * jmsService.setBasicConfig(getInputConfiguration());
     * assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
     * }
     * 
     * @Test
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
     * 
     * private OutputConfiguration getOutputConfiguration() {
     * OutputConfiguration basicConfig = new OutputConfiguration();
     * basicConfig.setModuleList(ACTIVEMQ);
     * basicConfig.setUrl(URL);
     * basicConfig.setDestination(DESTINATION);
     * basicConfig.setMessageType(MessageType.QUEUE);
     * basicConfig.setUserIdentity(true);
     * basicConfig.setUserName("admin");
     * basicConfig.setPassword("anything");
     * return basicConfig;
     * }
     * 
     * private InputMapperConfiguration getInputConfiguration() {
     * InputMapperConfiguration basicConfig = new InputMapperConfiguration();
     * basicConfig.setModuleList(ACTIVEMQ);
     * basicConfig.setUrl(URL);
     * basicConfig.setDestination(DESTINATION);
     * basicConfig.setMessageType(MessageType.QUEUE);
     * DurableSubscriptionConfiguration subsConfig = new DurableSubscriptionConfiguration();
     * subsConfig.setDurableSubscription(DURABLE_SUBSCRIPTION);
     * subsConfig.setClientId(CLIENT_ID);
     * subsConfig.setSubscriberName(SUBSCRIBER_NAME);
     * basicConfig.setSubscriptionConfig(subsConfig);
     * basicConfig.setTimeout(TIMEOUT);
     * return basicConfig;
     * }
     * 
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
