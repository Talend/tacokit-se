package org.talend.components.jms.service;

import org.junit.jupiter.api.Test;
import org.talend.components.jms.configuration.MessageType;
import org.talend.components.jms.output.OutputConfiguration;
import org.talend.components.jms.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.Assert.assertEquals;

@WithComponents("org.talend.components.jms") // component package
public class JMSServiceTest {

    public static final String URL = "tcp://localhost:61616";

    public static final String QUEUE_TEST = "test";

    public static final String TEST_MESSAGE = "hello world";

    public static final String ACTIVEMQ = "ACTIVEMQ";

    @Service
    private JmsService jmsService;

    @Test
    void sendAndReceiceJMSMessage() {
        OutputConfiguration configuration = new OutputConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setDestination(QUEUE_TEST);
        configuration.setMessageType(MessageType.QUEUE);
        jmsService.setConfiguration(configuration);
        jmsService.sendTextMessage(TEST_MESSAGE);

        assertEquals("Sent and received messages should be equal", TEST_MESSAGE, jmsService.receiveTextMessage());
    }

    @Test
    void receiveJMSMessage() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setDestination(QUEUE_TEST);
        configuration.setMessageType(MessageType.QUEUE);
        jmsService.setConfiguration(configuration);
        String message = jmsService.receiveTextMessage();

    }
}
