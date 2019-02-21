package org.talend.components.salesforce.service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.Iterator;
import java.util.Properties;

import javax.xml.namespace.QName;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;

import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SalesforceService {

    private static final int DEFAULT_TIMEOUT = 60000;

    private static final String CONFIG_FILE_lOCATION_KEY = "org.talend.component.salesforce.config.file";

    private static final String RETIRED_ENDPOINT = "www.salesforce.com";

    private static final String ACTIVE_ENDPOINT = "login.salesforce.com";

    private static final String DEFAULT_API_VERSION = "42.0";

    public static final String URL = "https://" + ACTIVE_ENDPOINT + "/services/Soap/u/" + DEFAULT_API_VERSION;

    private Properties loadCustomConfiguration(final LocalConfiguration configuration) {
        final String configFile = configuration.get(CONFIG_FILE_lOCATION_KEY);
        try (final InputStream is = configFile != null && !configFile.isEmpty() ? (new FileInputStream(configFile)) : null) {
            if (is != null) {
                return new Properties() {

                    {
                        load(is);
                    }
                };
            }
        } catch (final IOException e) {
            log.warn("not found the property file, will use the default value for endpoint and timeout", e);
        }

        return null;
    }

    public PartnerConnection connect(final BasicDataStore datastore, final LocalConfiguration localConfiguration)
            throws ConnectionException {
        final Properties props = loadCustomConfiguration(localConfiguration);
        final Integer timeout = (props != null) ? Integer.parseInt(props.getProperty("timeout", String.valueOf(DEFAULT_TIMEOUT)))
                : DEFAULT_TIMEOUT;
        final String endpoint = getEndpoint(props);
        ConnectorConfig config = newConnectorConfig(endpoint);
        config.setAuthEndpoint(endpoint);
        config.setUsername(datastore.getUserId());
        String password = datastore.getPassword();
        String securityKey = datastore.getSecurityKey();
        if (securityKey != null && !securityKey.trim().isEmpty()) {
            password = password + securityKey;
        }
        config.setPassword(password);
        config.setConnectionTimeout(timeout);
        config.setCompression(true);// This should only be false when doing debugging.
        config.setUseChunkedPost(true);
        config.setValidateSchema(false);

        // Notes on how to test this
        // http://thysmichels.com/2014/02/15/salesforce-wsc-partner-connection-session-renew-when-session-timeout/
        config.setSessionRenewer(connectorConfig -> {
            log.debug("renewing session...");
            SessionRenewer.SessionRenewalHeader header = new SessionRenewer.SessionRenewalHeader();
            connectorConfig.setSessionId(null);
            PartnerConnection connection;
            connection = new PartnerConnection(connectorConfig);
            header.name = new QName("urn:partner.soap.sforce.com", "SessionHeader");
            header.headerElement = connection.getSessionHeader();
            log.debug("session renewed!");
            return header;
        });
        return new PartnerConnection(config);
    }

    private String getEndpoint(final Properties props) {
        final String endpoint = props != null ? props.getProperty("endpoint", URL) : URL;
        if (endpoint.contains(RETIRED_ENDPOINT)) {
            return endpoint.replaceFirst(RETIRED_ENDPOINT, ACTIVE_ENDPOINT);
        }
        return endpoint;
    }

    private ConnectorConfig newConnectorConfig(final String ep) {
        return new ConnectorConfig() {

            @Override
            public Proxy getProxy() {
                final Iterator<Proxy> pxyIt = ProxySelector.getDefault().select(URI.create(ep)).iterator();
                return pxyIt.hasNext() ? pxyIt.next() : super.getProxy();
            }
        };
    }

    public BulkConnection bulkConnect(final BasicDataStore datastore, final LocalConfiguration configuration)
            throws AsyncApiException, ConnectionException {

        final Properties props = loadCustomConfiguration(configuration);
        final PartnerConnection partnerConnection = connect(datastore, configuration);
        final ConnectorConfig partnerConfig = partnerConnection.getConfig();
        ConnectorConfig bulkConfig = newConnectorConfig(getEndpoint(props));
        bulkConfig.setSessionId(partnerConfig.getSessionId());
        // For session renew
        bulkConfig.setSessionRenewer(partnerConfig.getSessionRenewer());
        bulkConfig.setUsername(partnerConfig.getUsername());
        bulkConfig.setPassword(partnerConfig.getPassword());
        bulkConfig.setAuthEndpoint(partnerConfig.getServiceEndpoint());

        // reuse proxy
        bulkConfig.setProxy(partnerConfig.getProxy());

        /*
         * The endpoint for the Bulk API service is the same as for the normal SOAP uri until the /Soap/ part. From here
         * it's '/async/versionNumber'
         */
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        partnerConfig.setAuthEndpoint(soapEndpoint);
        // set it by a default property file

        // Service endpoint should be like this:
        // https://ap1.salesforce.com/services/Soap/u/37.0/00D90000000eSq3
        String apiVersion = soapEndpoint.substring(soapEndpoint.lastIndexOf("/services/Soap/u/") + 17);
        apiVersion = apiVersion.substring(0, apiVersion.indexOf("/"));
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
        bulkConfig.setRestEndpoint(restEndpoint);
        bulkConfig.setCompression(true);// This should only be false when doing debugging.
        bulkConfig.setTraceMessage(false);
        bulkConfig.setValidateSchema(false);
        return new BulkConnection(bulkConfig);
    }

    public IllegalStateException handleConnectionException(final ConnectionException e) {
        if (e == null) {
            return new IllegalStateException("unexpected error. can't handle connection error.");
        } else if (ApiFault.class.isInstance(e)) {
            final ApiFault queryFault = ApiFault.class.cast(e);
            return new IllegalStateException(queryFault.getExceptionMessage(), queryFault);
        } else {
            return new IllegalStateException("connection error", e);
        }
    }
}
