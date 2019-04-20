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

package org.talend.components.mongodb.datastore;

import java.io.Serializable;
import java.util.List;

import lombok.Data;
import org.talend.components.mongodb.service.UIMongoDBService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataStore("MongoDBDatastore")
@Checkable(UIMongoDBService.HEALTH_CHECK)
@GridLayout({ @GridLayout.Row({ "useReplicaSetAddress" }), @GridLayout.Row({ "replicaAddresses" }),
        @GridLayout.Row({ "server", "port" }), @GridLayout.Row({ "database" }), @GridLayout.Row({ "useSSL" }),
        @GridLayout.Row({ "authentication" }), @GridLayout.Row({ "authenticationMechanism" }),
        @GridLayout.Row({ "kerberosCreds" }), @GridLayout.Row({ "username", "password" }),
        @GridLayout.Row({ "setAuthenticationDatabase" }), @GridLayout.Row({ "authenticationDatabase" }) })
@Documentation("Connection for MongoDB components")
public class MongoDBDatastore implements Serializable {

    @Option
    @Documentation("Use replica addresses instead of single server")
    private boolean useReplicaSetAddress;

    @Option
    @Documentation("Replica addresses set")
    @ActiveIf(target = "useReplicaSetAddress", value = "true")
    private List<ReplicaAddress> replicaAddresses;

    @Option
    @Documentation("Server address")
    @ActiveIf(target = "useReplicaSetAddress", value = "false")
    private String server;

    @Option
    @Documentation("Server port")
    @DefaultValue("27017")
    @ActiveIf(target = "useReplicaSetAddress", value = "false")
    private int port;

    @Option
    @Documentation("Database to use")
    private String database;

    @Option
    @Documentation("Enable the SSL or TLS encrypted connection.")
    private boolean useSSL;

    @Option
    @Documentation("Enable the database authentication.")
    private boolean authentication;

    @Option
    @ActiveIf(target = "authentication", value = "true")
    @Documentation("Among the mechanisms listed on the Authentication mechanism drop-down list, the NEGOTIATE one is recommended if you are not using Kerberos, because it automatically select the authentication mechanism the most adapted to the MongoDB version you are using.")
    @DefaultValue("NEGOTIATE_MEC")
    private AuthenticationMechanism authenticationMechanism;

    public enum AuthenticationMechanism {
        NEGOTIATE_MEC,
        PLAIN_MEC,
        SCRAMSHA1_MEC,
        KERBEROS_MEC
    }

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "authentication", value = "true"),
            @ActiveIf(target = "authenticationMechanism", value = { "NEGOTIATE_MEC", "SCRAMSHA1_MEC" }) })
    @Documentation("If the username to be used to connect to MongoDB has been created in a specific Authentication database of MongoDB, select this check box to enter the name of this Authentication database in the Authentication database field that is displayed.")
    private boolean setAuthenticationDatabase;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "authentication", value = "true"),
            @ActiveIf(target = "setAuthenticationDatabase", value = "true"),
            @ActiveIf(target = "authenticationMechanism", value = { "NEGOTIATE_MEC", "SCRAMSHA1_MEC" }) })
    @Documentation("Set MongoDB Authentication database")
    private String authenticationDatabase;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "authentication", value = "true"),
            @ActiveIf(target = "authenticationMechanism", value = { "NEGOTIATE_MEC", "SCRAMSHA1_MEC", "PLAIN_MEC" }) })
    @Documentation("Enter the username")
    private String username;

    @Option
    @Credential
    @ActiveIfs(value = { @ActiveIf(target = "authentication", value = "true"),
            @ActiveIf(target = "authenticationMechanism", value = { "NEGOTIATE_MEC", "SCRAMSHA1_MEC", "PLAIN_MEC" }) })
    @Documentation("Enter the password")
    private String password;

    @Option
    @Credential
    @ActiveIfs(value = { @ActiveIf(target = "authentication", value = "true"),
            @ActiveIf(target = "authenticationMechanism", value = { "KERBEROS_MEC" }) })
    @Documentation("Credentials for Kerberos authentication")
    private KerberosCredentials kerberosCreds;

}