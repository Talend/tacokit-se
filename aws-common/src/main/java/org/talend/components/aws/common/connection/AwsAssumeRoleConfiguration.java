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

import com.amazonaws.regions.Regions;
import org.talend.components.aws.common.service.UiAwsAuthenticationService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row("arn"), @GridLayout.Row("roleSessionName"), @GridLayout.Row("sessionDuration") }),
        @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "specifySTSEndpoint", "stsEndpoint" }),
                @GridLayout.Row({ "signingRegion" }), @GridLayout.Row({ "externalId" }), @GridLayout.Row({ "serialNumber" }),
                @GridLayout.Row({ "tokenCode" }), @GridLayout.Row({ "tags" }), @GridLayout.Row({ "policyArns" }),
                @GridLayout.Row({ "policy" }) }) })
public class AwsAssumeRoleConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Role ARN.")
    private String arn;

    @Option
    @Required
    @Documentation("Role session name.")
    private String roleSessionName;

    @Option
    @Documentation("Session duration.")
    private Integer sessionDuration = 15;

    @Option
    @Documentation("Set STS endpoint.")
    private boolean specifySTSEndpoint;

    @Option
    @Documentation("STS endpoint.")
    @ActiveIf(target = "specifySTSEndpoint", value = "true")
    private String stsEndpoint;

    @Option
    @Documentation("Signing region.")
    @Required
    @Proposable(UiAwsAuthenticationService.GET_AWS_REGIONS)
    private String signingRegion;

    @Option
    @Documentation("External Id.")
    private String externalId;

    @Option
    @Documentation("Serial number.")
    private String serialNumber;

    @Option
    @Documentation("Token code.")
    private String tokenCode;

    @Option
    @Documentation("Tags.")
    private List<AwsStsTag> tags;

    @Option
    @Documentation("IAM policy ARNs.")
    private List<AwsIamPolicyArn> policyArns;

    @Option
    @TextArea
    @Documentation("Policy.")
    private String policy;

}
