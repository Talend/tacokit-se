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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.json.bind.Jsonb;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.talend.components.jms.ProviderInfo;
import org.talend.components.jms.configuration.Configuration;
import org.talend.components.jms.configuration.MessageType;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JmsService {

    private final static String CONFIG_FILE_lOCATION_KEY = "org.talend.component.jms.config.file";

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

    /*
     * @Service
     * private I18nMessage i18n;
     */

    @Service
    private Configuration configuration;

    @Service
    private Resolver resolver;

    private Connection connection;

    private Context jndiContext;

    private Session session;

    private Map<String, ProviderInfo> loadProvidersFromConfigurationFile() {
        final Map<String, ProviderInfo> availableProviders = new HashMap<>();
        InputStream is = null;
        try {
            final String configFile = localConfiguration.get(CONFIG_FILE_lOCATION_KEY);
            if (configFile != null) {// priority to the system property
                try {
                    is = new FileInputStream(configFile);
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException(e);
                }
            } else {// then look in the classpath
                is = this.getClass().getClassLoader().getResourceAsStream("jms_config.json");
            }
            final List<ProviderInfo> info = jsonb.fromJson(is, providersType);
            availableProviders.putAll(info.stream().collect(toMap(ProviderInfo::getId, identity())));
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // no-op too bad but who care
                }
            }
        }

        return availableProviders;
    }

    private URLClassLoader getProviderClassLoader(final String prividerId) {
        return providersClassLoaders.computeIfAbsent(prividerId, key -> {
            final ProviderInfo providerInfo = getProviders().get(prividerId);
            final Collection<File> providerFiles = resolver.resolveFromDescriptor(new ByteArrayInputStream(
                    providerInfo.getPaths().stream().filter(p -> p.getPath() != null && !p.getPath().isEmpty())
                            .map(ProviderInfo.Path::getPath).collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
            // final String missingJars = providerFiles.stream().filter(f -> !f.exists()).map(File::getAbsolutePath)
            // .collect(joining("\n"));
            // if (!missingJars.isEmpty()) {
            // log.error(i18n.errorDriverLoad(driverId, missingJars));
            // return null;
            // }
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

    private Connection getConnection()
            throws InstantiationException, ClassNotFoundException, NamingException, IllegalAccessException, JMSException {
        if (connection != null) {
            return connection;
        }

        // create ConnectionFactory from JNDI
        ConnectionFactory connectionFactory = (ConnectionFactory) getJNDIContext().lookup(CONNECTION_FACTORY);
        connection = configuration.isUserIdentity()
                ? connectionFactory.createConnection(configuration.getUserName(), configuration.getPassword())
                : connectionFactory.createConnection();
        connection.start();

        return connection;
    }

    // create JNDI context
    private Context getJNDIContext()
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, NamingException {
        if (jndiContext != null) {
            return jndiContext;
        }

        Hashtable<String, String> properties = new Hashtable<>();
        properties.put(Context.PROVIDER_URL, configuration.getUrl());

        InitialContextFactory contextFactory = (InitialContextFactory) (getProviderClassLoader(configuration.getModuleList())
                .loadClass(getContextProvider()).newInstance());
        jndiContext = contextFactory.getInitialContext(properties);
        return jndiContext;
    }

    private MessageProducer createProducer() {
        MessageProducer producer = null;
        try {
            producer = getSession().createProducer(createDestination());
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return producer;
    }

    private MessageConsumer createConsumer() {
        MessageConsumer consumer = null;
        try {
            consumer = createConsumer(createDestination());
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return consumer;
    }

    private MessageConsumer createConsumer(Destination destination)
            throws ClassNotFoundException, NamingException, InstantiationException, IllegalAccessException, JMSException {
        /*
         * if (MessageType.QUEUE == configuration.getMessageType()) {
         * //getSession().createConsumer()
         * }
         */

        return getSession().createConsumer(destination);
    }

    private Message createTextMessage(String text)
            throws ClassNotFoundException, NamingException, InstantiationException, IllegalAccessException, JMSException {
        return getSession().createTextMessage(text);
    }

    public void sendTextMessage(String text) {
        try {
            createProducer().send(createTextMessage(text));
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public String receiveTextMessage() {
        String text = null;
        try {
            Message message = createConsumer().receive();
            if (message != null) {
                text = ((TextMessage) message).getText();
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return text;
    }

    private Session getSession()
            throws JMSException, InstantiationException, IllegalAccessException, NamingException, ClassNotFoundException {
        return session == null ? getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE) : session;
    }

    private String getContextProvider() {
        return getProviders().get(configuration.getModuleList()).getClazz();
    }

    private Destination createDestination(String destinationName)
            throws ClassNotFoundException, NamingException, InstantiationException, IllegalAccessException, JMSException {
        return MessageType.QUEUE == configuration.getMessageType() ? getSession().createQueue(destinationName)
                : getSession().createTopic(destinationName);
    }

    private Destination createDestination()
            throws NamingException, JMSException, InstantiationException, ClassNotFoundException, IllegalAccessException {
        return configuration.isUserJNDILookup() ? (javax.jms.Destination) getJNDIContext().lookup(configuration.getDestination())
                : createDestination(configuration.getDestination());
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

}
