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
package org.talend.components.magentocms.service.http;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.configurer.oauth1.OAuth1;

import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MagentoHttpClientService {

    private static final int ERROR_CODE_OK = 200;

    private static final int ERROR_CODE_BAD_REQUEST = 400;

    private static final int ERROR_CODE_UNAUTHORIZED = 401;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private MagentoHttpClient magentoHttpClient;

    @Service
    private AuthorizationHelper authorizationHelper;

    @Service
    private AuthorizationHandlerLoginPassword authorizationHandlerLoginPassword;

    public void setBase(String base) {
        magentoHttpClient.base(base);
    }

    public List<JsonObject> getRecords(MagentoDataStore magentoDataStore, String requestPath, Map<String, String> queryParameters)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        List<JsonObject> dataList;
        try {
            dataList = execGetRecords(magentoDataStore, requestPath, queryParameters);
            return dataList;
        } catch (UserTokenExpiredException e) {
            // try to get new token
            AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) magentoDataStore
                    .getAuthSettings();

            authorizationHandlerLoginPassword.clearTokenCache(authSettings);
            try {
                dataList = execGetRecords(magentoDataStore, requestPath, queryParameters);
                return dataList;
            } catch (UserTokenExpiredException e1) {
                throw new BadRequestException("User unauthorised exception");
            }
        }
    }

    private List<JsonObject> execGetRecords(MagentoDataStore magentoDataStore, String requestPath,
            Map<String, String> queryParameters) throws BadRequestException, UnknownAuthenticationTypeException, IOException,
            UserTokenExpiredException, BadCredentialsException {
        // escape '[', ']' in parameters for correct OAuth1 authentication
        Map<String, String> queryParametersOauth1 = urlEncodeParameters(queryParameters);

        Response<JsonObject> response;
        if (magentoDataStore.getAuthenticationType() == AuthenticationType.OAUTH_1) {
            OAuth1.Configuration oauth1Config = OAuth1.Configuration.builder()
                    .consumerKey(magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1ConsumerKey())
                    .consumerSecret(
                            magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1ConsumerSecret())
                    .token(magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1AccessToken())
                    .tokenSecret(
                            magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1AccessTokenSecret())
                    .build();
            response = magentoHttpClient.getRecords(requestPath, oauth1Config, queryParametersOauth1);
        } else {
            String auth = authorizationHelper.getAuthorization(magentoDataStore);
            response = magentoHttpClient.getRecords(requestPath, auth, queryParameters);
        }

        return processResponseGetRecords(response, magentoDataStore.getAuthenticationType());
    }

    private Map<String, String> urlEncodeParameters(Map<String, String> queryParameters) {
        return queryParameters.entrySet().stream().collect(Collectors.toMap(item -> {
            try {
                return URLEncoder.encode(item.getKey(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }, Map.Entry::getValue, (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        }, LinkedHashMap::new));
    }

    private List<JsonObject> processResponseGetRecords(Response<JsonObject> response, AuthenticationType authType)
            throws BadRequestException, UserTokenExpiredException {
        if (response.status() == ERROR_CODE_OK) {
            List<JsonObject> dataList = new ArrayList<>();
            response.body().getJsonArray("items").forEach((t) -> {
                dataList.add(t.asJsonObject());
            });
            return dataList;
        } else if (response.status() == ERROR_CODE_BAD_REQUEST) {
            throw handleBadRequest(response.error(JsonObject.class), null);
        } else if (response.status() == ERROR_CODE_UNAUTHORIZED && authType == AuthenticationType.LOGIN_PASSWORD) {
            // maybe token is expired
            throw new UserTokenExpiredException();
        } else {
            throw new BadRequestException("unknown exception");
        }
    }

    public JsonObject postRecords(MagentoDataStore magentoDataStore, String requestPath, JsonObject dataList)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        try {
            JsonObject res = execPostRecords(magentoDataStore, requestPath, dataList);
            return res;
        } catch (UserTokenExpiredException e) {
            // try to get new token
            AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) magentoDataStore
                    .getAuthSettings();

            authorizationHandlerLoginPassword.clearTokenCache(authSettings);
            try {
                JsonObject res = execPostRecords(magentoDataStore, requestPath, dataList);
                return res;
            } catch (UserTokenExpiredException e1) {
                throw new BadRequestException("User unauthorised exception");
            }
        }
    }

    private JsonObject execPostRecords(MagentoDataStore magentoDataStore, String requestPath, JsonObject dataList)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, UserTokenExpiredException,
            BadCredentialsException {
        Response<JsonObject> response;

        if (magentoDataStore.getAuthenticationType() == AuthenticationType.OAUTH_1) {
            OAuth1.Configuration oauth1Config = OAuth1.Configuration.builder()
                    .consumerKey(magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1ConsumerKey())
                    .consumerSecret(
                            magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1ConsumerSecret())
                    .token(magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1AccessToken())
                    .tokenSecret(
                            magentoDataStore.getAuthenticationOauth1Configuration().getAuthenticationOauth1AccessTokenSecret())
                    .build();
            response = magentoHttpClient.postRecords(requestPath, oauth1Config, dataList);
        } else {
            String auth = authorizationHelper.getAuthorization(magentoDataStore);
            response = magentoHttpClient.postRecords(requestPath, auth, dataList);
        }

        return processResponsePostRecords(response, dataList, magentoDataStore.getAuthenticationType());
    }

    private JsonObject processResponsePostRecords(Response<JsonObject> response, JsonObject dataList, AuthenticationType authType)
            throws BadRequestException, UserTokenExpiredException {
        if (response.status() == ERROR_CODE_OK) {
            return response.body();
        } else if (response.status() == ERROR_CODE_BAD_REQUEST) {
            throw handleBadRequest(response.error(JsonObject.class), dataList.toString());
        } else if (response.status() == ERROR_CODE_UNAUTHORIZED && authType == AuthenticationType.LOGIN_PASSWORD) {
            // maybe token is expired
            throw new UserTokenExpiredException();
        } else {
            throw new BadRequestException("unknown exception");
        }
    }

    public String getToken(String requestPath, String login, String password) {
        final JsonObject body = jsonBuilderFactory.createObjectBuilder().add("username", login).add("password", password).build();
        Response<JsonValue> response = magentoHttpClient.getToken(requestPath, body);
        String accessToken = null;
        if (response.status() == ERROR_CODE_OK) {
            JsonValue responseBody = response.body();
            // convert json-string to string
            accessToken = responseBody.toString().replaceAll("\"", "");
        }
        return accessToken;
    }

    /**
     * process messages like this:
     * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
     */
    private BadRequestException handleBadRequest(JsonObject errorObject, String requestObject) throws BadRequestException {
        /*
         * process messages like this:
         * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
         */
        String message = errorObject.getJsonString("message").getString();
        if (errorObject.get("parameters") != null) {
            if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.OBJECT) {
                for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
                    message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
                }
            } else if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.ARRAY) {
                JsonArray params = errorObject.getJsonArray("parameters");
                for (int i = 0; i < params.size(); i++) {
                    message = message.replaceAll("%" + (i + 1), params.getString(i));
                }
            }
        }
        return new BadRequestException(
                "An error occurred: " + message + (requestObject == null ? "" : "For object: " + requestObject));
    }
}
