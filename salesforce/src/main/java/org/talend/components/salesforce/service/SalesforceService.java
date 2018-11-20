/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.namespace.QName;

import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.soql.FieldDescription;
import org.talend.components.salesforce.soql.SoqlQuery;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SalesforceService {

    // TODO maybe no need for this
    public static final String CONFIG_FILE_lOCATION_KEY = "org.talend.component.salesforce.config.file";

    public static final String RETIRED_ENDPOINT = "www.salesforce.com";

    public static final String ACTIVE_ENDPOINT = "login.salesforce.com";

    public static final String DEFAULT_API_VERSION = "42.0";

    public static final String URL = "https://" + ACTIVE_ENDPOINT + "/services/Soap/u/" + DEFAULT_API_VERSION;

    /** Properties file key for endpoint storage. */
    public static final String ENDPOINT_PROPERTY_KEY = "endpoint";

    public static final String TIMEOUT_PROPERTY_KEY = "timeout";

    private static final int DEFAULT_TIMEOUT = 60000;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000Z'");

    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS'Z'");

    /**
     * Load Dataprep user Salesforce properties file for endpoint & timeout default values.
     *
     * @return a {@link Properties} objet (maybe empty) but never null.
     */
    private Properties loadCustomConfiguration(final LocalConfiguration configuration) {
        final String configFile = configuration.get(CONFIG_FILE_lOCATION_KEY);
        try (final InputStream is =
                configFile != null && !configFile.isEmpty() ? (new FileInputStream(configFile)) : null) {
            if (is != null) {
                return new Properties() {

                    {
                        load(is);
                    }
                };
            } else {
                log.warn("not found the property file, will use the default value for endpoint and timeout");
            }
        } catch (final IOException e) {
            log.warn("not found the property file, will use the default value for endpoint and timeout", e);
        }

        return null;
    }

    public PartnerConnection connect(final BasicDataStore datastore, final LocalConfiguration localConfiguration)
            throws ConnectionException {
        final Properties props = loadCustomConfiguration(localConfiguration);
        final Integer timeout = (props != null)
                ? Integer.parseInt(props.getProperty(TIMEOUT_PROPERTY_KEY, String.valueOf(DEFAULT_TIMEOUT)))
                : DEFAULT_TIMEOUT;
        ConnectorConfig config = newConnectorConfig(datastore.getEndpoint());
        config.setAuthEndpoint(datastore.getEndpoint());
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

    /**
     * Return the datastore endpoint, loading a default value if no value is present.
     *
     * @return the datastore endpoint value.
     */
    protected String getEndpoint(final LocalConfiguration localConfiguration) {

        final Properties props = loadCustomConfiguration(localConfiguration);
        if (props != null) {
            String endpointProp = props.getProperty(ENDPOINT_PROPERTY_KEY);
            if (endpointProp != null && !endpointProp.isEmpty()) {
                if (endpointProp.contains(RETIRED_ENDPOINT)) {
                    endpointProp = endpointProp.replaceFirst(RETIRED_ENDPOINT, ACTIVE_ENDPOINT);
                }
                return endpointProp;
            }
        }
        return URL;
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
        ConnectorConfig bulkConfig = newConnectorConfig(datastore.getEndpoint());
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

    public Map<String, Field> getFieldMap(BasicDataStore dataStore, String moduleName,
            final LocalConfiguration localConfiguration) {
        try {
            PartnerConnection connection = connect(dataStore, localConfiguration);
            DescribeSObjectResult module = connection.describeSObject(moduleName);
            Map<String, Field> fieldMap = new HashMap<>();
            for (Field field : module.getFields()) {
                fieldMap.put(field.getName(), field);
            }
            return fieldMap;

        } catch (ConnectionException e) {
            throw handleConnectionException(e);
        }
    }

    public static void addField(final Record.Builder builder, Field field, final String value) {
        if (value == null || field == null) {
            return;
        }
        try {
            switch (field.getType()) {
            case _boolean:
                builder.withBoolean(field.getName(), Boolean.valueOf(value));
                break;
            case _double:
            case percent:
                builder.withDouble(field.getName(), Double.parseDouble(value));
                break;
            case _int:
                builder.withInt(field.getName(), Integer.valueOf(value));
                break;
            case currency:
                builder.withDouble(field.getName(), Double.parseDouble(value));
                break;
            case date:
                builder.withDateTime(field.getName(), DATE_FORMAT.parse(value));
                break;
            case datetime:
                builder.withTimestamp(field.getName(), DATETIME_FORMAT.parse(value).getTime());
                break;
            case time:
                builder.withTimestamp(field.getName(), TIME_FORMAT.parse(value).getTime());
                break;
            case base64:
            default:
                builder.withString(field.getName(), value);
                break;
            }
        } catch (ParseException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    public static String guessModuleName(String soqlQuery) {
        SoqlQuery query = SoqlQuery.getInstance();
        query.init(soqlQuery);

        List<FieldDescription> fieldDescriptions = query.getFieldDescriptions();
        return query.getDrivingEntityName();

    }
}
