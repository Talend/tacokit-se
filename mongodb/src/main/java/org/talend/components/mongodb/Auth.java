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
package org.talend.components.mongodb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "needAuth" }), @GridLayout.Row({ "authMech" }), @GridLayout.Row({ "useAuthDatabase" }),
                @GridLayout.Row({ "authDatabase" }), @GridLayout.Row({ "username" }), @GridLayout.Row({ "password" }) }) })
@Documentation("Path Mapping")
public class Auth implements Serializable {

    @Option
    @Documentation("Need auth")
    private boolean needAuth;

    @Option
    @ActiveIf(target = "needAuth", value = "true")
    @Documentation("Need auth")
    private AuthMech authMech = AuthMech.NEGOTIATE;

    @Option
    @ActiveIf(target = "needAuth", value = "true")
    @ActiveIf(target = "authMech", value = { "NEGOTIATE", "SCRAM_SHA_1_SASL" })
    @Documentation("Use auth database")
    private boolean useAuthDatabase;

    @Option
    @ActiveIf(target = "needAuth", value = "true")
    @ActiveIf(target = "authMech", value = { "NEGOTIATE", "SCRAM_SHA_1_SASL" })
    @ActiveIf(target = "useAuthDatabase", value = "true")
    @Documentation("Auth database")
    private String authDatabase;

    @Option
    @ActiveIf(target = "needAuth", value = "true")
    @Documentation("Username")
    private String username;

    @Option
    @ActiveIf(target = "needAuth", value = "true")
    @Documentation("Password")
    @Credential
    private String password;

}
