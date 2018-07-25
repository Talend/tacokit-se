package org.talend.components.jms.service;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.Test;
import org.talend.components.jms.output.OutputOutputConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;

@WithComponents("org.talend.components.jms") // component package
public class JMSServiceTest {

    @Service
    private OutputOutputConfiguration configuration;

    public static final String ACTIVEMQ = "ACTIVEMQ";

    @Service
    private JmsService jmsService;

    @Test
    void sendJMSMessage() throws JMSException {
        OutputOutputConfiguration configuration = new OutputOutputConfiguration();
        configuration.setModuleList(ACTIVEMQ);
        configuration.setUrl("tcp://localhost:61616");
        configuration.setTo("test");
        configuration.setMessageType(OutputOutputConfiguration.MessageType.QUEUE);
        jmsService.setConfiguration(configuration);
        MessageProducer producer = jmsService.createProducer();

        producer.send(createActiveMQMessage("hello world"));
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
