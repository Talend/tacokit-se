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
package org.talend.components.rest.configuration.auth.oauth2;

import java.io.Serializable;

import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@NoArgsConstructor
@AllArgsConstructor
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "grantType" }),
        @GridLayout.Row({ "passwordGrant" }), @GridLayout.Row({ "authMethod" }), })
public class Oauth2 implements Serializable {

    @Option
    @DefaultValue("Password")
    @Documentation("")
    private GrantType grantType;

    @Option
    @ActiveIf(target = "grantType", value = "Password")
    @Documentation("")
    private PasswordGrant passwordGrant;

    @Option
    @DefaultValue("Header")
    @Documentation("")
    private AuthMethod authMethod;

    public enum GrantType {
        Password
    }

    public enum AuthMethod {
        Url,
        Header
    }

}
