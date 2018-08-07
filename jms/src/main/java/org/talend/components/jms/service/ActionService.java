package org.talend.components.jms.service;

import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.components.jms.source.InputMapperConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.NamingException;
import java.net.URLClassLoader;
import java.util.Collections;

import static java.util.stream.Collectors.toList;

@Service
public class ActionService {

    public static final String ACTION_LIST_SUPPORTED_BROKER = "ACTION_LIST_SUPPORTED_BROKER";

    public static final String ACTION_BASIC_HEALTH_CHECK = "ACTION_BASIC_HEALTH_CHECK";

    @Service
    private JmsService jmsService;

    @Service
    private I18nMessage i18n;

    @DynamicValues(ACTION_LIST_SUPPORTED_BROKER)
    public Values loadSupportedJMSProviders() {
        return new Values(jmsService.getProviders().keySet().stream().map(id -> new Values.Item(id, id)).collect(toList()));
    }

    @DiscoverSchema("discoverSchema")
    public Schema guessSchema(InputMapperConfiguration config) {
        return new Schema(Collections.singletonList(new Schema.Entry("messageContent", Type.STRING)));
    }

    @HealthCheck(ACTION_BASIC_HEALTH_CHECK)
    public HealthCheckStatus validateBasicDatastore(@Option final JmsDataStore datastore) throws NamingException {
        if (datastore.getUrl() == null || datastore.getUrl().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyURL());
        }
        final URLClassLoader loader = jmsService.getProviderClassLoader(datastore.getModuleList());
        if (loader == null) {
            throw new IllegalStateException(i18n.errorProviderLoad(datastore.getModuleList(), null));
        }

        Context jndiContext = null;
        Connection connection = null;

        try {
            // create JNDI context
            jndiContext = jmsService.getJNDIContext(datastore.getUrl(),
                    datastore.getModuleList()
            );
            // create ConnectionFactory from JNDI
            ConnectionFactory connectionFactory = jmsService.getConnectionFactory(jndiContext);

            try {
                connection = jmsService.getConnection(connectionFactory,
                        datastore.isUserIdentity(),
                        datastore.getUserName(),
                        datastore.getPassword());
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorInvalidConnection());
            }

            try {
                connection.start();
            } catch (JMSException e) {
                throw new IllegalStateException(i18n.errorStartMessagesDelivery());
            }

        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.errorInstantiateConnectionFactory(e.getMessage()));
        } finally {
            jmsService.closeConnection(connection);
            jmsService.closeContext(jndiContext);
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

}
