/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.runtime.client;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

/**
 * Holds information required for logging in of a client in NetSuite.
 */
@Data
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class NetSuiteCredentials {

    private String email;

    private String password;

    private String account;

    private String roleId;

    private String applicationId;

    private int numberOfSeats = 1;

    private String id;

    private String companyId;

    private String userId;

    private boolean useSsoLogin = false;

    public NetSuiteCredentials(String email, String password, String account, String roleId, String applicationId) {
        this(email, password, account, roleId, applicationId, 1);
    }

    public NetSuiteCredentials(String email, String password, String account, String roleId, String applicationId,
            int numberOfSeats) {
        this.email = email;
        this.password = password;
        this.account = account;
        this.roleId = roleId;
        this.applicationId = applicationId;
        this.numberOfSeats = numberOfSeats;
    }

    public static NetSuiteCredentials loadFromLocation(URI location, String propertyPrefix) throws IOException {
        InputStream stream;
        if (location.getScheme().equals("classpath")) {
            stream = NetSuiteCredentials.class.getResourceAsStream(location.getSchemeSpecificPart());
        } else {
            stream = location.toURL().openStream();
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } finally {
            stream.close();
        }
        return loadFromProperties(properties, propertyPrefix);
    }

    /**
     * Load credentials from plain {@code Properties}.
     *
     * @param properties properties object
     * @param prefix prefix for property keys, can be empty
     * @return credentials object
     */
    public static NetSuiteCredentials loadFromProperties(Properties properties, String prefix) {
        NetSuiteCredentials credentials = new NetSuiteCredentials();
        credentials.setEmail(properties.getProperty(prefix + "email"));
        credentials.setPassword(properties.getProperty(prefix + "password"));
        credentials.setRoleId(properties.getProperty(prefix + "roleId"));
        credentials.setAccount(properties.getProperty(prefix + "account"));
        credentials.setApplicationId(properties.getProperty(prefix + "applicationId"));
        return credentials;
    }

}