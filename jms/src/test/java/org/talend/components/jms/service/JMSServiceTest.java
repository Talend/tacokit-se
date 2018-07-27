package org.talend.components.jms.service;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.Test;
import org.talend.components.jms.output.OutputConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;

import static org.junit.Assert.assertEquals;

@WithComponents("org.talend.components.jms") // component package
public class JMSServiceTest {

    public static final String URL = "tcp://localhost:61616";

    public static final String QUEUE_TEST = "test";

    public static final String TEST_MESSAGE = "hello world";

    @Service
    private OutputConfiguration configuration;

    public static final String ACTIVEMQ = "ACTIVEMQ";

    @Service
    private JmsService jmsService;

    @Test
    void sendAndReceiceJMSMessage() throws JMSException {
        OutputConfiguration configuration = new OutputConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl(URL);
        configuration.setTo(QUEUE_TEST);
        configuration.setMessageType(OutputConfiguration.MessageType.QUEUE);
        jmsService.setConfiguration(configuration);
        MessageProducer producer = jmsService.createProducer();
        producer.send(createActiveMQMessage(TEST_MESSAGE));

        assertEquals("Sent and received messages should be equal", TEST_MESSAGE, ((ActiveMQTextMessage) jmsService.receiveMessage()).getText());
    }

    private Message createActiveMQMessage(String text) {
        Message message = new ActiveMQTextMessage();
        try {
            ((ActiveMQTextMessage) message).setText(text);
        } catch (MessageNotWriteableException e) {
            e.printStackTrace();
        }
        return message;
    }

}
