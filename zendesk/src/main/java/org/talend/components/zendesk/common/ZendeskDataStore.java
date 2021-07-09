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
package org.talend.components.zendesk.common;

import java.io.Serializable;

import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@DataStore(ConfigurationHelper.DATA_STORE_ID)
@Checkable(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
@GridLayout({ @GridLayout.Row({ "serverUrl" }), @GridLayout.Row({ "authenticationType" }),
        @GridLayout.Row({ "authenticationLoginPasswordConfiguration" }),
        @GridLayout.Row({ "authenticationApiTokenConfiguration" }) })
@Documentation("Data store settings. Zendesk's server connection and authentication preferences.")
public class ZendeskDataStore implements Serializable {

    @Option
    @Documentation("Zendesk server URL.")
    @Pattern("^(http://|https://).*")
    private String serverUrl = "";

    @Option
    @Documentation("Authentication type (Login etc.).")
    private AuthenticationType authenticationType = AuthenticationType.LOGIN_PASSWORD;

    @Option
    @Documentation("Authentication Login settings.")
    @ActiveIf(target = "authenticationType", value = { "LOGIN_PASSWORD" })
    private AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration;

    @Option
    @Documentation("Authentication API token settings.")
    @ActiveIf(target = "authenticationType", value = { "API_TOKEN" })
    private AuthenticationApiTokenConfiguration authenticationApiTokenConfiguration;

}