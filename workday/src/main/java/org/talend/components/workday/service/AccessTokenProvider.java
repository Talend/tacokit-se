package org.talend.components.workday.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.http.*;

import javax.json.JsonObject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;


public interface AccessTokenProvider extends HttpClient {

    static final Logger log = LoggerFactory.getLogger(AccessTokenProvider.class);

    @UseConfigurer(AccessTokenConfigurer.class)
    @Request(path = "v1/token", method = "POST")
    Response<JsonObject>  getAuthorizationToken(@Header("Authorization") String authId,
                                                String payload);

    default Token getAccessToken(WorkdayDataStore ds) {
        Instant nowUTC = Instant.now();

        this.base(ds.getAuthEndpoint());

        final String payload = "tenant_alias=" + ds.getTenantAlias() + "&grant_type=client_credentials";

        Response<JsonObject>  result = this.getAuthorizationToken(ds.getAuthorizationHeader(), payload);
        if (result.status() / 100 != 2) {
            String errorLib = result.error(String.class);
            log.error("Error while trying get token : HTTP {} : {}", result.status(), errorLib);
            throw new WorkdayException(errorLib);
        }
        JsonObject json = result.body();

        final String accessToken = json.getString("access_token");
        final String tokenType = json.getString("token_type");
        final String expireIn = json.getString("expires_in");

        Instant expireAt = nowUTC.plus(Integer.parseInt(expireIn), ChronoUnit.SECONDS);

        return new Token(accessToken, tokenType, expireAt);
    }
}
