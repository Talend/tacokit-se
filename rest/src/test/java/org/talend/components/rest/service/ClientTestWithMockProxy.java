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
package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.configuration.auth.Basic;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * When -Dtalend.junit.http.capture=true is given
 * use -Dorg.talend.components.common.service.http.digest.authorization_header=_Authorization_
 * to change the name of the header, if not, tck proxy will exclude this header.
 */
@Slf4j
@WithComponents(value = "org.talend.components.rest")
// @HttpApi(useSsl = true)
public class ClientTestWithMockProxy {

    @Injected
    private BaseComponentsHandler handler;

    @Service
    RestService service;

    private RequestConfig config;

    @BeforeEach
    void before() {
        config = RequestConfigBuilderTest.getEmptyRequestConfig();
    }

    // @Test
    public void testDigestAuthWithQopPostMan() {

        String user = "postman";
        String pwd = "password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Digest);
        auth.setBasic(basic);
        config.getDataset().getDatastore().setAuthentication(auth);

        config.getDataset().getDatastore().setBase("https://postman-echo.com");
        config.getDataset().setResource("digest-auth");
        config.getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);

        Record resp = service.execute(config);
        assertEquals(200, resp.getInt("status"));
    }

}
