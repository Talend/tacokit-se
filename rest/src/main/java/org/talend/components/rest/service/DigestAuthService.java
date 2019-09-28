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

import org.talend.component.common.service.http.BasicHeader;
import org.talend.component.common.service.http.DigestAuthContext;
import org.talend.component.common.service.http.DigestScheme;
import org.talend.component.common.service.http.UserNamePassword;
import org.talend.sdk.component.api.service.http.HttpException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.talend.sdk.component.api.service.http.Response;

public class DigestAuthService {

    public Response call(UserNamePassword cred, Supplier<Response> call) {

        int status = -1;
        Map<String, List<String>> headers = Collections.emptyMap();
        Response response = null;
        try {
            try {
                response = call.get();
                if (Response.class.isInstance(response)) {
                    final Response resp = Response.class.cast(response);
                    status = resp.status();
                    headers = resp.headers();
                }
                // return response;
            } catch (final HttpException he) {
                response = he.getResponse();
                status = response.status();
                headers = response.headers();
            }

            if (status == 401) {
                // If status == 401 try to achieve the challenge
                List<String> lwa = Optional.ofNullable(headers.get("WWW-Authenticate")).orElse(Collections.emptyList());
                if (lwa.size() >= 0) {
                    // if WWW-Authenticate exists
                    BasicHeader authChallenge = new BasicHeader("WWW-Authenticate", lwa.get(0));
                    DigestScheme scheme = new DigestScheme();
                    DigestAuthContext threadLocalContext = DigestAuthContext.getThreadLocalContext();
                    String digest = scheme.createDigestResponse(cred.getUser(), cred.getPassword(), authChallenge,
                            threadLocalContext); // compute header
                    threadLocalContext.setDigestAuthHeader(digest);

                    response = call.get();

                }
            }
        } catch (DigestScheme.AuthenticationException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            DigestAuthContext.removeThreadLocalContext();
        }

        return response;
    }

}
