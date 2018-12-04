// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jms.service;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.IllegalStateException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.json.bind.Jsonb;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.talend.components.jms.ProviderInfo;
import org.talend.components.jms.configuration.MessageType;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JmsService {

    private final static String CONFIG_FILE_LOCATION_KEY = "org.talend.component.jms.config.file";

    private final static String CONNECTION_FACTORY = "ConnectionFactory";

    private final ParameterizedType providersType = new ParameterizedType() {

        @Override
        public Type[] getActualTypeArguments() {
            return new Type[] { ProviderInfo.class };
        }

        @Override
        public Type getRawType() {
            return List.class;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }
    };

    @Getter(lazy = true)
    private final Map<String, ProviderInfo> providers = loadProvidersFromConfigurationFile();

    private final Map<String, URLClassLoader> providersClassLoaders = new HashMap<>();

    @Service
    private Jsonb jsonb;

    @Service
    private LocalConfiguration localConfiguration;

    @Service
    private I18nMessage i18n;

    @Service
    private Resolver resolver;

    private Map<String, ProviderInfo> loadProvidersFromConfigurationFile() {
        final Map<String, ProviderInfo> availableProviders = new HashMap<>();
        final String configFile = localConfiguration.get(CONFIG_FILE_LOCATION_KEY);

        try (InputStream is = configFile != null ? new FileInputStream(configFile)
                : this.getClass().getClassLoader().getResourceAsStream("jms_config.json")) {
            final List<ProviderInfo> info = jsonb.fromJson(is, providersType);
            availableProviders.putAll(info.stream().collect(toMap(ProviderInfo::getId, identity())));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return availableProviders;
    }

    public URLClassLoader getProviderClassLoader(final String providerId) {
        return providersClassLoaders.computeIfAbsent(providerId, key -> {
            final ProviderInfo providerInfo = getProviders().get(providerId);
            if (providerInfo == null) {
                throw new IllegalStateException(i18n.errorLoadProvider(providerId, null));
            }
            final Collection<File> providerFiles = resolver.resolveFromDescriptor(new ByteArrayInputStream(
                    providerInfo.getPaths().stream().filter(p -> p.getPath() != null && !p.getPath().isEmpty())
                            .map(ProviderInfo.Path::getPath).collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
            final String missingJars = providerFiles.stream().filter(f -> !f.exists()).map(File::getAbsolutePath)
                    .collect(joining("\n"));
            if (!missingJars.isEmpty()) {
                log.error(i18n.errorLoadProvider(providerId, missingJars));
                return null;
            }
            final URL[] urls = providerFiles.stream().filter(File::exists).map(f -> {
                try {
                    return f.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new IllegalStateException(e);
                }
            }).toArray(URL[]::new);
            return new URLClassLoader(urls, this.getClass().getClassLoader());
        });
    }

    public Context getJNDIContext(String url, String moduleList)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, NamingException {
        Hashtable<String, String> properties = new Hashtable<>();
        properties.put(Context.PROVIDER_URL, url);

        InitialContextFactory contextFactory = (InitialContextFactory) (getProviderClassLoader(moduleList)
                .loadClass(getProviders().get(moduleList).getClazz()).newInstance());
        return contextFactory.getInitialContext(properties);
    }

    public Destination getDestination(Session session, String destination, MessageType messageType) throws JMSException {
        return (MessageType.QUEUE == messageType) ? session.createQueue(destination) : session.createTopic(destination);
    }

    public Session getSession(Connection connection) throws JMSException {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public ConnectionFactory getConnectionFactory(Context context) throws NamingException {
        return (ConnectionFactory) context.lookup(CONNECTION_FACTORY);
    }

    public Connection getConnection(ConnectionFactory connectionFactory, boolean isUserIdentity, String userName, String password)
            throws JMSException {
        return isUserIdentity ? connectionFactory.createConnection(userName, password) : connectionFactory.createConnection();
    }

    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
    }

    public void closeSession(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                log.warn(i18n.warnSessionCantBeClosed(), e);
            }
        }
    }

    public void closeProducer(MessageProducer producer) {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException e) {
                log.warn(i18n.warnProducerCantBeClosed(), e);
            }
        }
    }

    public void closeConsumer(MessageConsumer consumer) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConsumerCantBeClosed(), e);
            }
        }
    }

    public void closeContext(Context context) {
        if (context != null) {
            try {
                context.close();
            } catch (NamingException e) {
                log.warn(i18n.warnJNDIContextCantBeClosed(), e);
            }
        }
    }

}
