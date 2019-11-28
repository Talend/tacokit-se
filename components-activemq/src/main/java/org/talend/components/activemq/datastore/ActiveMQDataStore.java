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
package org.talend.components.activemq.datastore;

import lombok.Data;
import org.talend.components.activemq.configuration.Broker;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;
import java.util.StringJoiner;

import static org.talend.components.activemq.service.ActionService.ACTION_BASIC_HEALTH_CHECK;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.AND;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.OR;

@Data
@GridLayout({ @GridLayout.Row({ "failover" }), @GridLayout.Row({ "staticDiscovery" }), @GridLayout.Row({ "SSL" }),
        @GridLayout.Row({ "failoverURIParameters" }), @GridLayout.Row({ "staticDiscoveryURIParameters" }),
        @GridLayout.Row({ "brokers" }), @GridLayout.Row({ "host", "port" }), @GridLayout.Row("userIdentity"),
        @GridLayout.Row({ "userName", "password" }), @GridLayout.Row({ "transacted" }) })
@DataStore("basic")
@Checkable(ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class ActiveMQDataStore implements Serializable {

    @Option
    @ActiveIf(target = "staticDiscovery", value = "false")
    @Documentation("Failover mode checkbox")
    private Boolean failover = false;

    @Option
    @ActiveIf(target = "failover", value = "false")
    @Documentation("Discovery mode checkbox")
    private Boolean staticDiscovery = false;

    @Option
    @Documentation("SSL mode checkbox")
    private Boolean SSL = false;

    @Option
    @ActiveIf(target = "failover", value = "true")
    @Documentation("Failover mode parameters")
    private String failoverURIParameters = "?randomize=false";

    @Option
    @ActiveIf(target = "staticDiscovery", value = "true")
    @Documentation("Static discovery mode parameters")
    private String staticDiscoveryURIParameters = "?transport.maxReconnectDelay=5000&transport.useExponentialBackOff=false";

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "failover", value = "true"),
            @ActiveIf(target = "staticDiscovery", value = "true") }, operator = OR)
    @Documentation("List of brokers for static discovery or failover")
    private List<Broker> brokers;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "failover", value = "false"),
            @ActiveIf(target = "staticDiscovery", value = "false") }, operator = AND)
    @Documentation("Input for JMS server Broker")
    private String host;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "failover", value = "false"),
            @ActiveIf(target = "staticDiscovery", value = "false") }, operator = AND)
    @Documentation("Input for JMS server Broker")
    private Integer port;

    @Option
    @Documentation("Checkbox for User login/password checking")
    private boolean userIdentity = false;

    @Option
    @Documentation("Input for User Name")
    @ActiveIf(target = "userIdentity", value = "true")
    private String userName;

    @Option
    @Credential
    @Documentation("Input for password")
    @ActiveIf(target = "userIdentity", value = "true")
    private String password;

    @Option
    @Documentation("Checkbox for transactions support")
    private Boolean transacted = false;

    public String getUrl() {
        String url;
        if (!getFailover() && !getStaticDiscovery()) {
            url = getBrokerURL(getSSL(), getHost(), getPort());
        } else {
            StringJoiner brokerURLs = new StringJoiner(",");
            for (Broker brokerBroker : getBrokers()) {
                brokerURLs.add(getBrokerURL(getSSL(), brokerBroker.getHost(), brokerBroker.getPort()));
            }
            url = getTransport() + ":(" + brokerURLs + ")" + getURIParameters();
        }
        return url;
    }

    private String getURIParameters() {
        String URIParameters = "";
        if (getFailover()) {
            URIParameters = getFailoverURIParameters();
        }
        if (getStaticDiscovery()) {
            URIParameters = getStaticDiscoveryURIParameters();
        }
        return URIParameters;
    }

    private String getBrokerURL(Boolean isSSLUsed, String host, Integer port) {
        return isSecured(isSSLUsed) + "://" + host + ":" + port;
    }

    private String getTransport() {
        String transport = null;
        if (getFailover()) {
            transport = "failover";
        }
        if (getStaticDiscovery()) {
            transport = "discovery://static";
        }
        return transport;
    }

    private String isSecured(boolean sslTransport) {
        return sslTransport ? "ssl" : "tcp";
    }
}
