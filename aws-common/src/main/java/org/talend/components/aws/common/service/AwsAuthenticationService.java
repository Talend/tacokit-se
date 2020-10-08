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
package org.talend.components.aws.common.service;

import org.talend.components.aws.common.connection.AwsAssumeRoleConfiguration;
import org.talend.components.aws.common.connection.AwsAuthenticationConfiguration;
import org.talend.components.aws.common.connection.AwsBasicAuthenticationConfiguration;
import org.talend.components.aws.common.connection.AwsStsTag;
import org.talend.sdk.component.api.service.Service;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.PolicyDescriptorType;
import com.amazonaws.services.securitytoken.model.Tag;
import com.amazonaws.util.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class AwsAuthenticationService implements Serializable {

    public AWSCredentialsProvider getCredentialsProvider(AwsAuthenticationConfiguration configuration) {
        AWSCredentialsProvider credentialsProvider;
        if (configuration.isInheritCredentials()) {
            credentialsProvider = new EC2ContainerCredentialsProviderWrapper();
        } else {
            credentialsProvider = getBasicAwsCredentials(configuration.getBasicAuthConfig());
        }
        if (configuration.isAssumeRole()) {
            credentialsProvider = getAssumeRoleCredentialsProvider(credentialsProvider,
                    configuration.getAssumeRoleConfiguration());
        }
        return credentialsProvider;
    }

    public AWSCredentialsProvider getBasicAwsCredentials(AwsBasicAuthenticationConfiguration basicConfig) {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(basicConfig.getAccessKey(), basicConfig.getSecretKey()));
    }

    protected AWSCredentialsProvider getAssumeRoleCredentialsProvider(AWSCredentialsProvider credentialsProvider,
            AwsAssumeRoleConfiguration assumeRoleConfiguration) {
        AWSSecurityTokenService stsClient = createStsClient(credentialsProvider, assumeRoleConfiguration);
        AssumeRoleRequest assumeRoleRequest = createAssumeRoleRequest(assumeRoleConfiguration);
        AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);
        Credentials assumeRoleCreds = assumeRoleResult.getCredentials();
        return new AWSStaticCredentialsProvider(new BasicSessionCredentials(assumeRoleCreds.getAccessKeyId(),
                assumeRoleCreds.getSecretAccessKey(), assumeRoleCreds.getSessionToken()));
    }

    protected AWSSecurityTokenService createStsClient(AWSCredentialsProvider credentialsProvider,
            AwsAssumeRoleConfiguration assumeRoleConfiguration) {
        AWSSecurityTokenServiceClientBuilder stsClientBuilder = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(credentialsProvider);
        if (assumeRoleConfiguration.isSpecifySTSEndpoint()) {
            stsClientBuilder.withEndpointConfiguration(new EndpointConfiguration(assumeRoleConfiguration.getStsEndpoint(),
                    assumeRoleConfiguration.getSigningRegion()));
        } else {
            stsClientBuilder.withRegion(assumeRoleConfiguration.getSigningRegion());
        }
        return stsClientBuilder.build();
    }

    protected AssumeRoleRequest createAssumeRoleRequest(AwsAssumeRoleConfiguration assumeRoleConfiguration) {
        int sessionDurationSeconds = assumeRoleConfiguration.getSessionDuration() == null ? 15 * 60
                : assumeRoleConfiguration.getSessionDuration() * 60;
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withDurationSeconds(sessionDurationSeconds)
                .withRoleArn(assumeRoleConfiguration.getArn()).withRoleSessionName(assumeRoleConfiguration.getRoleSessionName());
        if (StringUtils.hasValue(assumeRoleConfiguration.getSerialNumber())) {
            assumeRoleRequest.withSerialNumber(assumeRoleConfiguration.getSerialNumber());
        }
        if (StringUtils.hasValue(assumeRoleConfiguration.getExternalId())) {
            assumeRoleRequest.withExternalId(assumeRoleConfiguration.getExternalId());
        }
        if (StringUtils.hasValue(assumeRoleConfiguration.getTokenCode())) {
            assumeRoleRequest.withTokenCode(assumeRoleConfiguration.getTokenCode());
        }
        if (StringUtils.hasValue(assumeRoleConfiguration.getPolicy())) {
            assumeRoleRequest.withPolicy(assumeRoleConfiguration.getPolicy());
        }
        if (assumeRoleConfiguration.getTags() != null && !assumeRoleConfiguration.getTags().isEmpty()) {
            List<Tag> tags = new ArrayList<>();
            List<String> transitiveTags = new ArrayList<>();
            for (AwsStsTag tag : assumeRoleConfiguration.getTags()) {
                tags.add(new Tag().withKey(tag.getKey()).withValue(tag.getValue()));
                if (tag.isTransitive()) {
                    transitiveTags.add(tag.getKey());
                }
            }
            if (!transitiveTags.isEmpty()) {
                assumeRoleRequest.withTransitiveTagKeys(transitiveTags);
            }
            assumeRoleRequest.withTags(tags);
        }
        if (assumeRoleConfiguration.getPolicyArns() != null && !assumeRoleConfiguration.getPolicyArns().isEmpty()) {
            List<PolicyDescriptorType> arnPolicies = assumeRoleConfiguration.getPolicyArns().stream()
                    .map(s -> new PolicyDescriptorType().withArn(s.getPolicyArn())).collect(Collectors.toList());
            assumeRoleRequest.setPolicyArns(arnPolicies);
        }
        return assumeRoleRequest;
    }
}
