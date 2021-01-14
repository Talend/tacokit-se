/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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

import lombok.Data;
import org.talend.components.mongodb.AddressType;
import org.talend.components.mongodb.Address;
import org.talend.components.mongodb.Auth;
import org.talend.components.mongodb.ConnectionParameter;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Version(1)
@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "mongodb")
@DataStore("MongoDBDataStore")
@Checkable("healthCheck")
@GridLayout(names = GridLayout.FormType.MAIN, value = { @GridLayout.Row({ "addressType" }), @GridLayout.Row({ "address" }),
        @GridLayout.Row({ "replicaSetAddress" }), @GridLayout.Row({ "database" }), @GridLayout.Row({ "auth" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "connectionParameter" }) })
@Documentation("MongoDB connection")
public class MongoDBDataStore implements Serializable {

    @Option
    @Required
    @Documentation("https://docs.mongodb.com/manual/reference/connection-string/")
    private AddressType addressType = AddressType.STANDALONE;

    @Option
    @ActiveIf(target = "addressType", value = "STANDALONE")
    @Documentation("https://docs.mongodb.com/manual/reference/connection-string/")
    private Address address;

    @Option
    @ActiveIf(target = "addressType", value = "REPLICA_SET")
    @Documentation("https://docs.mongodb.com/manual/reference/connection-string/")
    private List<Address> replicaSetAddress = Collections.emptyList();

    /*
     * @Option
     * 
     * @ActiveIf(target = "addressType", value = "SHARDED_CLUSTER")
     * 
     * @Documentation("https://docs.mongodb.com/manual/reference/connection-string/")
     * private List<Address> shardedClusterAddress = Collections.emptyList();
     */

    @Option
    @Required
    @Documentation("Database")
    private String database;

    // TODO have to support the function for locate a file for cert if need cert? or trust any cert, not good for me?
    // maybe remove it
    /*
     * @Option
     * 
     * @Documentation("Use SSL")
     * private boolean useSSL;
     */

    @Option
    @Documentation("auth page")
    private Auth auth;

    // advanced page
    @Option
    @Documentation("Connection parameter")
    private List<ConnectionParameter> connectionParameter = Collections.emptyList();
}