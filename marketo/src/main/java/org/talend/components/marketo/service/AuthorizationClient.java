// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.service;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CLIENT_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CLIENT_SECRET;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CODE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_GRANT_TYPE;

import java.util.regex.Pattern;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

public interface AuthorizationClient extends HttpClient {

    String CLIENT_CREDENTIALS = "client_credentials";

    String ERROR_TOKEN_EXPIRED = "602";

    String RECOVERABLE_ERRORS_PATTERN = "(502|604|606|608|611|614|615)";

    /**
     * Retrieve an access token from Marketo Idnetity Endpoint.
     *
     * @param grantType constant "client_credentials"
     * @param clientId Client identifier
     * @param clientSecret Client secret code
     * @return Marketo authorization token for API
     */
    @Request(path = "/identity/oauth/token")
    Response<JsonObject> getAuthorizationToken( //
            @Query(ATTR_GRANT_TYPE) String grantType, //
            @Query(ATTR_CLIENT_ID) String clientId, //
            @Query(ATTR_CLIENT_SECRET) String clientSecret//
    );

    default String getAccessToken(MarketoDataStore dataStore) {
        Response<JsonObject> result = getAuthorizationToken(CLIENT_CREDENTIALS, dataStore.getClientId(),
                dataStore.getClientSecret());
        if (result.status() == 200) {
            return result.body().getString(ATTR_ACCESS_TOKEN);
        }
        return null;
    }

    /**
     * Checks in returned errors if access token is expired (602 Access token expired The Access Token included in the call
     * is no longer valid due to expiration.)
     * 
     * @param errors list of API errors
     * @return
     */
    default boolean isAccessTokenExpired(JsonArray errors) {
        if (errors != null) {
            for (JsonObject error : errors.getValuesAs(JsonObject.class)) {
                if (ERROR_TOKEN_EXPIRED.equals(error.getString(ATTR_CODE))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns true if error is recoverable (we can retry operation).
     *
     * Potential recoverable errors returned by API:
     *
     * <li>502 Bad Gateway The remote server returned an error. Likely a timeout. The request should be retried with
     * exponential backoff.</li>
     * <li>604 Request timed out The request was running for too long, or exceeded the time-out period specified in the
     * header of the call.</li>
     * <li>606 Max rate limit ‘%s’ exceeded with in ‘%s’ secs The number of calls in the past 20 seconds was greater than
     * 100</li>
     * <li>608 API Temporarily Unavailable</li>
     * <li>611 System error All unhandled exceptions</li>
     * <li>614 Invalid Subscription The destination subscription cannot be found or is unreachable. This usually indicates
     * temporary inaccessibility.</li>
     * <li>615 Concurrent access limit reached At most 10 requests can be processed by any subscription at a time. This will
     * be returned if there are already 10 requests for the subscription ongoing.</li>
     *
     * @param errors list of API errors
     * @return
     */
    default boolean isErrorRecoverable(JsonArray errors) {
        final Pattern pattern = Pattern.compile(RECOVERABLE_ERRORS_PATTERN);
        for (JsonObject error : errors.getValuesAs(JsonObject.class)) {
            if (pattern.matcher(error.getString(ATTR_CODE)).matches()) {
                return true;
            }
        }
        return false;
    }

}
