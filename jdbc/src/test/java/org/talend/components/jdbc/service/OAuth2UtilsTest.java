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
package org.talend.components.jdbc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.components.jdbc.datastore.GrantType;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.jdbc")
public class OAuth2UtilsTest {

    @Service
    private I18nMessage i18n;

    private TokenClientFake tokenClientFake = new TokenClientFake();

    @ParameterizedTest
    @CsvFileSource(files = "src/test/resources/test-data-for-oauth-utils-test.csv")
    public void testFailedToGetAccessToken(ArgumentsAccessor arguments) {
        Exception exception = assertThrows(ComponentException.class,
                () -> OAuth2Utils.getAccessToken(createJdbcConnectionProperties(arguments), tokenClientFake, i18n));
        assertEquals(arguments.getString(7), exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource(value = { "https://talend.com,correct,correct,CLIENT_CREDENTIALS,correct" })
    public void testAccessTokenSuccess(ArgumentsAccessor arguments) {
        assertEquals(TokenClientFake.ACCESS_TOKEN,
                OAuth2Utils.getAccessToken(createJdbcConnectionProperties(arguments), tokenClientFake, i18n));
    }

    private JdbcConnection createJdbcConnectionProperties(ArgumentsAccessor arguments) {
        JdbcConnection connectionProperties = new JdbcConnection();
        connectionProperties.setOauthTokenEndpoint(arguments.getString(0));
        connectionProperties.setClientId(arguments.getString(1));
        connectionProperties.setClientSecret(arguments.getString(2));
        connectionProperties.setGrantType(GrantType.valueOf(arguments.getString(3)));
        connectionProperties.setScope(arguments.getString(4));
        if (arguments.size() > 5) {
            connectionProperties.setOauthUsername(arguments.getString(5));
            connectionProperties.setOauthPassword(arguments.getString(6));
        }
        return connectionProperties;
    }

}
