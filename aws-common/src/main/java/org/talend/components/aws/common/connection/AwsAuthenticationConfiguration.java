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
package org.talend.components.aws.common.connection;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import lombok.Data;

@Data
@GridLayouts({
        @GridLayout({ @GridLayout.Row("basicAuthConfig"), @GridLayout.Row("inheritCredentials"), @GridLayout.Row("assumeRole"),
                @GridLayout.Row("assumeRoleConfiguration") }),
        @GridLayout(names = FormType.ADVANCED, value = @GridLayout.Row("assumeRoleConfiguration")) })
public class AwsAuthenticationConfiguration implements Serializable {

    @Option
    @Documentation("Basic authentication configuration.")
    @ActiveIf(target = "inheritCredentials", value = "false")
    private AwsBasicAuthenticationConfiguration basicAuthConfig;

    @Option
    @Documentation("Inherit credentials from AWS role.")
    private boolean inheritCredentials;

    @Option
    @Documentation("Assume role.")
    private boolean assumeRole;

    @Option
    @Documentation("Assume role configuration.")
    @ActiveIf(target = "assumeRole", value = "true")
    private AwsAssumeRoleConfiguration assumeRoleConfiguration;

}
