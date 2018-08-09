package org.talend.components.jms.source;

import java.io.Serializable;
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
import javax.naming.Context;
import javax.naming.NamingException;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jms.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import org.talend.components.jms.service.JmsService;

import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;

@Slf4j
@Documentation("TODO fill the documentation for this source")
public class InputSource implements Serializable {

    private final InputMapperConfiguration configuration;

    private final JmsService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    private int counter;

    private Connection connection;

    private Context jndiContext;

    private Session session;

    private Destination destination;

    private MessageConsumer consumer;

    private final I18nMessage i18n;

    public InputSource(@Option final InputMapperConfiguration configuration, final JmsService service,
            final JsonBuilderFactory jsonBuilderFactory, final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        try {
            // create JNDI context
            jndiContext = service.getJNDIContext(configuration.getConnection().getUrl(),
                    configuration.getConnection().getModuleList());
            // create ConnectionFactory from JNDI
            ConnectionFactory connectionFactory = service.getConnectionFactory(jndiContext);

            try {
                connection = service.getConnection(connectionFactory, configuration.getConnection().isUserIdentity(),
                        configuration.getConnection().getUserName(), configuration.getConnection().getPassword());
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorInvalidConnection());
            }

            if (configuration.getSubscriptionConfig().isDurableSubscription()) {
                connection.setClientID(configuration.getSubscriptionConfig().getClientId());
            }

            try {
                connection.start();
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorStartMessagesDelivery());
            }

            session = service.getSession(connection);

            destination = service.getDestination(session, jndiContext, configuration.getBasicConfig().getDestination(),
                    configuration.getBasicConfig().getMessageType());

            if (configuration.getSubscriptionConfig().isDurableSubscription()) {
                consumer = session.createDurableSubscriber((Topic) destination,
                        configuration.getSubscriptionConfig().getSubscriberName(), configuration.getMessageSelector(), false);
            } else {
                consumer = session.createConsumer(destination, configuration.getMessageSelector());
            }

        } catch (JMSException e) {
            throw new IllegalStateException(i18n.errorCreateJMSInstance());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NamingException e) {
            throw new IllegalStateException(i18n.errorInstantiateConnectionFactory(e.getMessage()));
        }
    }

    @Producer
    public JsonObject next() {

        String textMessage = null;
        if (counter >= configuration.getMaximumMessages()) {
            return null;
        } else {

            Message message = null;
            try {
                message = consumer.receive(configuration.getTimeout() * 1000);
                if (message != null) {
                    textMessage = ((TextMessage) message).getText();
                    message.acknowledge();
                }
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorCantReceiveMessage());
            }
            counter++;
            return message != null ? buildJSON(textMessage) : null;

        }
    }

    private JsonObject buildJSON(String text) {
        JsonObjectBuilder recordBuilder = jsonBuilderFactory.createObjectBuilder();
        recordBuilder.add(MESSAGE_CONTENT, text);
        return recordBuilder.build();
    }

    @PreDestroy
    public void release() {
        service.closeConsumer(consumer);
        service.closeSession(session);
        service.closeConnection(connection);
        service.closeContext(jndiContext);
    }
}