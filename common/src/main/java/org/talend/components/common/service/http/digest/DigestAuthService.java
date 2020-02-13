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
package org.talend.components.common.service.http.digest;

import org.talend.components.common.service.http.common.BasicHeader;
import org.talend.sdk.component.api.service.http.HttpException;
import org.talend.sdk.component.api.service.http.Response;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class DigestAuthService {

    public Response call(DigestAuthContext context, Supplier<Response> supplier) {

        int status = -1;
        Map<String, List<String>> headers = Collections.emptyMap();
        Response response = null;
        try {
            try {
                response = supplier.get();
                if (response != null) {
                    status = response.status();
                    headers = response.headers();
                }
            } catch (final HttpException he) {
                response = he.getResponse();
                status = response.status();
                headers = response.headers();
            }

            if (status == 401) {
                // If status == 401 try to achieve the challenge
                List<String> lwa = Optional.ofNullable(headers.get("WWW-Authenticate")).orElse(Collections.emptyList());
                if (lwa.size() > 0) {
                    // if WWW-Authenticate exists
                    BasicHeader authChallenge = new BasicHeader("WWW-Authenticate", lwa.get(0));
                    DigestScheme scheme = new DigestScheme();
                    String digest = scheme.createDigestResponse(context.getCredentials().getUser(),
                            context.getCredentials().getPassword(), authChallenge, context); // compute header
                    context.setDigestAuthHeader(digest);

                    response = supplier.get();

                }
            }
        } catch (DigestScheme.AuthenticationException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            context.setDigestAuthHeader(null);
        }

        return response;
    }

}
