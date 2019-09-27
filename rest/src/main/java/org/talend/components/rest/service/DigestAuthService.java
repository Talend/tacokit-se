package org.talend.components.rest.service;

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
                //return response;
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
                    DigestScheme scheme = new DigestScheme();
                    scheme.initPreemptive(cred.getUser(), cred.getPassword(), );
                    String digest = scheme.ge "authent"; // compute header
                    DigestAuthContext threadLocalContext = DigestAuthContext.getThreadLocalContext();
                    threadLocalContext.setDigestAuthHeader(digest);

                    response = call.get();

                }
            }
        } finally {
            DigestAuthContext.removeThreadLocalContext();
        }

        return response;
    }

}
