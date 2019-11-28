/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.activemq.source;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.activemq.service.ActiveMQService;
import org.talend.components.activemq.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import static org.talend.components.activemq.MessageConst.MESSAGE_CONTENT;

@Slf4j
@Documentation("Main class for JMSInput records processing")
public class InputSource implements Serializable {

    private final InputMapperConfiguration configuration;

    private final ActiveMQService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    private int counter = 0;

    private Connection connection;

    private Session session;

    private Destination destination;

    private MessageConsumer consumer;

    private final I18nMessage i18n;

    private Queue<String> messages = new LinkedList<>();

    public InputSource(@Option final InputMapperConfiguration configuration, final ActiveMQService service,
            final JsonBuilderFactory jsonBuilderFactory, final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {

        // create ConnectionFactory
        ConnectionFactory connectionFactory = service.createConnectionFactory(configuration.getBasicConfig().getConnection());
        try {
            try {
                connection = service.getConnection(connectionFactory, configuration.getBasicConfig().getConnection());
            } catch (JMSException e) {
                throw new IllegalStateException(e.getMessage());
            }

            if (configuration.getSubscriptionConfig().isDurableSubscription()) {
                connection.setClientID(configuration.getSubscriptionConfig().getClientId());
            }

            try {
                connection.start();
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorStartMessagesDelivery());
            }

            session = service.getSession(connection, configuration.getBasicConfig().getConnection().getTransacted());

            destination = service.getDestination(session, configuration.getBasicConfig().getDestination(),
                    configuration.getBasicConfig().getMessageType());

            if (configuration.getSubscriptionConfig().isDurableSubscription()) {
                consumer = session.createDurableSubscriber((Topic) destination,
                        configuration.getSubscriptionConfig().getSubscriberName(), configuration.getMessageSelector(), false);
            } else {
                consumer = session.createConsumer(destination, configuration.getMessageSelector());
            }

        } catch (JMSException e) {
            throw new IllegalStateException(i18n.errorCreateJMSInstance());
        }
    }

    @Producer
    public JsonObject next() {
        if (counter >= configuration.getMaximumMessages()) {
            return null;
        } else {
            String message = receiveMessage();
            counter++;
            return message != null ? buildJSON(message) : null;
        }
    }

    private String receiveMessage() {
        Message message;
        String textMessage = null;
        try {
            message = consumer.receive(configuration.getTimeout() * 1000);
            if (message != null) {
                textMessage = ((TextMessage) message).getText();
            }
        } catch (JMSException e) {
            log.error(i18n.errorCantReceiveMessage(), e);
        }
        return textMessage;
    }

    private JsonObject buildJSON(String text) {
        JsonObjectBuilder recordBuilder = jsonBuilderFactory.createObjectBuilder();
        recordBuilder.add(MESSAGE_CONTENT, text);
        return recordBuilder.build();
    }

    @PreDestroy
    public void release() {
        service.closeConnection(connection);
    }
}