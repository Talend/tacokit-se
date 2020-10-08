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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.aws.common.connection.AwsAssumeRoleConfiguration;
import org.talend.components.aws.common.connection.AwsAuthenticationConfiguration;
import org.talend.components.aws.common.connection.AwsBasicAuthenticationConfiguration;
import org.talend.components.aws.common.connection.AwsIamPolicyArn;
import org.talend.components.aws.common.connection.AwsStsTag;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.Tag;

public class AwsAuthenticationServiceTest {

    private final AwsAuthenticationService service = new AwsAuthenticationService();

    private final static String ACCESS_KEY = "accessKey";

    private final static String SECRET_KEY = "secret";

    @Test
    public void testGetBasicAwsCredentials() {
        AWSCredentialsProvider credentialsProvider = service.getBasicAwsCredentials(createBasicAuth(ACCESS_KEY, SECRET_KEY));
        AWSCredentials credentials = credentialsProvider.getCredentials();

        assertThat(credentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
        Assertions.assertEquals(ACCESS_KEY, credentials.getAWSAccessKeyId());
        Assertions.assertEquals(SECRET_KEY, credentials.getAWSSecretKey());
    }

    @Test
    public void testBasicGetCredentialsProvider() {
        AwsAuthenticationConfiguration authConfig = new AwsAuthenticationConfiguration();
        authConfig.setBasicAuthConfig(createBasicAuth(ACCESS_KEY, SECRET_KEY));
        AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider(authConfig);
        AWSCredentials credentials = credentialsProvider.getCredentials();

        assertThat(credentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
        Assertions.assertEquals(ACCESS_KEY, credentials.getAWSAccessKeyId());
        Assertions.assertEquals(SECRET_KEY, credentials.getAWSSecretKey());
    }

    @Test
    public void testInheritCredentialsGetCredentialsProvider() {
        AwsAuthenticationConfiguration authConfig = new AwsAuthenticationConfiguration();
        authConfig.setInheritCredentials(true);
        AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider(authConfig);

        assertThat(credentialsProvider, instanceOf(EC2ContainerCredentialsProviderWrapper.class));
    }

    @Test
    public void testCreateAssumeRoleRequest() {
        AwsAssumeRoleConfiguration configuration = new AwsAssumeRoleConfiguration();
        configuration.setRoleSessionName("sessionName");
        configuration.setArn("arn");
        configuration.setExternalId("externalId");
        configuration.setPolicy("jsonPolicy");
        configuration.setSerialNumber("serialNumber");
        configuration.setSessionDuration(10);
        configuration.setSpecifySTSEndpoint(true);
        configuration.setStsEndpoint("stsEndpoint");
        configuration.setTokenCode("tokenCode");
        AwsIamPolicyArn iamPolicyArn = new AwsIamPolicyArn();
        iamPolicyArn.setPolicyArn("policyArn");
        configuration.setPolicyArns(Arrays.asList(iamPolicyArn));
        AwsStsTag transitiveTag = new AwsStsTag();
        transitiveTag.setKey("transitiveKey");
        transitiveTag.setValue("transitiveValue");
        transitiveTag.setTransitive(true);
        AwsStsTag tag = new AwsStsTag();
        tag.setKey("tagKey");
        tag.setValue("tagValue");
        tag.setTransitive(false);
        configuration.setTags(Arrays.asList(tag, transitiveTag));
        AssumeRoleRequest request = service.createAssumeRoleRequest(configuration);

        Assertions.assertEquals(600, request.getDurationSeconds());
        Assertions.assertEquals("sessionName", request.getRoleSessionName());
        Assertions.assertEquals("arn", request.getRoleArn());
        Assertions.assertEquals("externalId", request.getExternalId());
        Assertions.assertEquals("jsonPolicy", request.getPolicy());
        Assertions.assertEquals("serialNumber", request.getSerialNumber());
        Assertions.assertEquals("tokenCode", request.getTokenCode());
        Assertions.assertEquals(1, request.getPolicyArns().size());
        Assertions.assertEquals("policyArn", request.getPolicyArns().get(0).getArn());
        Assertions.assertEquals(2, request.getTags().size());
        Collection<Tag> expectedTags = Stream.of(tag, transitiveTag)
                .map(t -> new Tag().withKey(t.getKey()).withValue(t.getValue())).collect(Collectors.toList());
        assertThat(request.getTags(), containsInAnyOrder(expectedTags.toArray()));
        assertThat(request.getTransitiveTagKeys(), containsInAnyOrder(transitiveTag.getKey()));
    }

    @Test
    public void testCreateAssumeRoleRequestWithNullValues() {
        AwsAssumeRoleConfiguration configuration = new AwsAssumeRoleConfiguration();
        configuration.setRoleSessionName("sessionName");
        configuration.setArn("arn");
        AssumeRoleRequest request = service.createAssumeRoleRequest(configuration);

        Assertions.assertEquals(15 * 60, request.getDurationSeconds());
        Assertions.assertEquals("sessionName", request.getRoleSessionName());
        Assertions.assertEquals("arn", request.getRoleArn());
        Assertions.assertNull(request.getExternalId());
        Assertions.assertNull(request.getPolicy());
        Assertions.assertNull(request.getSerialNumber());
        Assertions.assertNull(request.getTokenCode());
        Assertions.assertNull(request.getPolicyArns());
        Assertions.assertNull(request.getTags());
    }

    @Test
    public void testGetCredentialsProviderAssumeRole() {
        AWSSecurityTokenService tokenService = Mockito.mock(AWSSecurityTokenService.class);
        Mockito.when(tokenService.assumeRole(Mockito.any())).thenReturn(new AssumeRoleResult().withCredentials(new Credentials()
                .withAccessKeyId("accessKeyId").withSecretAccessKey("secretAccessKey").withSessionToken("sessToken")));
        AwsAuthenticationService service = Mockito.spy(AwsAuthenticationService.class);

        AwsAuthenticationConfiguration authConfig = new AwsAuthenticationConfiguration();
        AwsAssumeRoleConfiguration assumeRoleConfiguration = new AwsAssumeRoleConfiguration();
        assumeRoleConfiguration.setRoleSessionName("sessionName");
        assumeRoleConfiguration.setArn("arn");
        authConfig.setAssumeRole(true);
        authConfig.setInheritCredentials(true);
        authConfig.setAssumeRoleConfiguration(assumeRoleConfiguration);
        Mockito.doReturn(tokenService).when(service).createStsClient(Mockito.any(), Mockito.any());

        AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider(authConfig);

        assertThat(credentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
        assertThat(credentialsProvider.getCredentials(), instanceOf(BasicSessionCredentials.class));
        BasicSessionCredentials credentials = (BasicSessionCredentials) credentialsProvider.getCredentials();
        Assertions.assertEquals("accessKeyId", credentials.getAWSAccessKeyId());
        Assertions.assertEquals("secretAccessKey", credentials.getAWSSecretKey());
        Assertions.assertEquals("sessToken", credentials.getSessionToken());
    }

    private AwsBasicAuthenticationConfiguration createBasicAuth(String accessKey, String secret) {
        AwsBasicAuthenticationConfiguration basicConfig = new AwsBasicAuthenticationConfiguration();
        basicConfig.setSecretKey(secret);
        basicConfig.setAccessKey(accessKey);
        return basicConfig;
    }

}
