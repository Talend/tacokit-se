package org.talend.components.jms.output;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.json.JsonObject;
import javax.naming.Context;
import javax.naming.NamingException;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jms.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;

import org.talend.components.jms.service.JmsService;
import org.talend.sdk.component.api.service.Service;

import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "JMSOutput")
@Processor(name = "Output")
@Documentation("TODO fill the documentation for this processor")
public class Output implements Serializable {

    private final OutputConfiguration configuration;

    private final I18nMessage i18n;

    @Service
    private final JmsService service;

    private Connection connection;

    private Context jndiContext;

    private Session session;

    private Destination destination;

    private MessageProducer producer;

    public Output(@Option("configuration") final OutputConfiguration configuration, final JmsService service,
            final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
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

            try {
                connection.start();
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorStartMessagesDelivery());
            }

            session = service.getSession(connection);

            destination = service.getDestination(session, configuration.getBasicConfig().getDestination(),
                    configuration.getBasicConfig().getMessageType());

            producer = session.createProducer(destination);
            producer.setDeliveryMode(configuration.getDeliveryMode().getIntValue());
        } catch (JMSException e) {
            throw new IllegalStateException(i18n.errorCreateJMSInstance());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NamingException e) {
            throw new IllegalStateException(i18n.errorInstantiateConnectionFactory(e.getMessage()));
        }

    }

    @ElementListener
    public void onNext(@Input final JsonObject record) {
        try {
            producer.send(session.createTextMessage(getMessage(record)));
        } catch (JMSException e) {
            throw new IllegalStateException(i18n.errorCantSendMessage());
        }
    }

    private String getMessage(JsonObject record) {
        return record.getString(MESSAGE_CONTENT);
    }

    @PreDestroy
    public void release() {
        service.closeProducer(producer);
        service.closeSession(session);
        service.closeConnection(connection);
        service.closeContext(jndiContext);
    }
}