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
package org.talend.components.adlsgen2.common.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpRequestBase;
import org.talend.components.adlsgen2.common.connection.Constants;
import org.talend.components.adlsgen2.common.connection.Constants.HeaderConstants;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * SharedKeyCredentials are a means of signing and authenticating storage requests. The key can be obtained from the
 * Azure portal. This factory will create policies which take care of all the details of creating strings to sign,
 * signing them, and setting the Authentication header. While this is a common way of authenticating with the service,
 * recommended practice is using .
 */
@Slf4j
public final class SharedKeyUtils {

    private final String accountName;

    private final byte[] accountKey;

    private HttpRequestBase requestBase;

    /**
     * Initializes a new instance of SharedKeyCredentials contains an account's name and its primary or secondary
     * accountKey.
     *
     * @param accountName The account name associated with the request.
     * @param accountKey The account access key used to authenticate the request.
     * @throws InvalidKeyException Thrown when the accountKey is ill-formatted.
     */
    public SharedKeyUtils(String accountName, String accountKey) throws InvalidKeyException {
        this.accountName = accountName;
        this.accountKey = Base64.getDecoder().decode(accountKey);

    }

    /**
     * Gets the account name associated with the request.
     *
     * @return The account name.
     */
    public String getAccountName() {
        return accountName;
    }

    /**
     * Constructs a canonicalized string for signing a request.
     *
     * @return A canonicalized string.
     */
    private String buildStringToSign() {

        String contentLength = getStandardHeaderValue("contentLength");
        contentLength = contentLength.equals("0") ? Constants.EMPTY_STRING : contentLength;

        return String.join("\n", requestBase.getMethod(), getStandardHeaderValue(HeaderConstants.CONTENT_ENCODING),
                getStandardHeaderValue(HeaderConstants.CONTENT_LANGUAGE), contentLength,
                getStandardHeaderValue(HeaderConstants.CONTENT_MD5), getStandardHeaderValue(HeaderConstants.CONTENT_TYPE),
                // x-ms-date header exists, so don't sign date header
                Constants.EMPTY_STRING, getStandardHeaderValue(HeaderConstants.IF_MODIFIED_SINCE),
                getStandardHeaderValue(HeaderConstants.IF_MATCH), getStandardHeaderValue(HeaderConstants.IF_NONE_MATCH),
                getStandardHeaderValue(HeaderConstants.IF_UNMODIFIED_SINCE), getStandardHeaderValue(HeaderConstants.RANGE),
                getAdditionalXmsHeaders(), getCanonicalizedResource(requestBase.getURI()));
    }

    private void appendCanonicalizedElement(final StringBuilder builder, final String element) {
        builder.append("\n");
        builder.append(element);
    }

    private String getAdditionalXmsHeaders() {
        // Add only headers that begin with 'x-ms-'
        Header[] allHeaders = requestBase.getAllHeaders();
        final ArrayList<String> xmsHeaderNameArray = new ArrayList<>();
        for (Header header : allHeaders) {
            String lowerCaseHeader = header.getName().toLowerCase(Locale.ROOT);
            if (lowerCaseHeader.startsWith(Constants.PREFIX_FOR_STORAGE_HEADER)) {
                xmsHeaderNameArray.add(lowerCaseHeader);
            }
        }

        if (xmsHeaderNameArray.isEmpty()) {
            return Constants.EMPTY_STRING;
        }

        Collections.sort(xmsHeaderNameArray);

        final StringBuilder canonicalizedHeaders = new StringBuilder();
        for (final String key : xmsHeaderNameArray) {
            if (canonicalizedHeaders.length() > 0) {
                canonicalizedHeaders.append('\n');
            }

            canonicalizedHeaders.append(key);
            canonicalizedHeaders.append(':');
            canonicalizedHeaders.append(requestBase.getFirstHeader(key).getValue());
        }

        return canonicalizedHeaders.toString();
    }

    /**
     * Canonicalized the resource to sign.
     *
     * @param requestURI A {@code java.net.URL} of the request.
     * @return The canonicalized resource to sign.
     */
    private String getCanonicalizedResource(URI requestURI) {

        // Resource path
        final StringBuilder canonicalizedResource = new StringBuilder("/");
        canonicalizedResource.append(this.accountName);

        // Note that AbsolutePath starts with a '/'.
        if (requestURI.getPath().length() > 0) {
            canonicalizedResource.append(requestURI.getPath());
        } else {
            canonicalizedResource.append('/');
        }

        // check for no query params and return
        if (requestURI.getQuery() == null) {
            return canonicalizedResource.toString();
        }

        Map<String, List<String>> queryParams = new HashMap<>();

        try {
            String query = java.net.URLDecoder.decode(requestURI.getQuery(), StandardCharsets.UTF_8.name());
            Arrays.stream(query.split("&")).forEach(e -> {
                int i = e.indexOf("=");
                queryParams.put(e.substring(0, i), Arrays.asList(e.substring(i + 1).split(",")));
            });
        } catch (UnsupportedEncodingException e) {
            // not going to happen - value came from JDK's own StandardCharsets
        }

        ArrayList<String> queryParamNames = new ArrayList<>(queryParams.keySet());
        Collections.sort(queryParamNames);

        for (String queryParamName : queryParamNames) {
            final List<String> queryParamValues = queryParams.get(queryParamName);
            Collections.sort(queryParamValues);
            String queryParamValuesStr = String.join(",", queryParamValues.toArray(new String[] {}));
            canonicalizedResource.append("\n").append(queryParamName.toLowerCase(Locale.ROOT)).append(":")
                    .append(queryParamValuesStr);
        }

        // append to main string builder the join of completed params with new line
        return canonicalizedResource.toString();
    }

    /**
     * Returns the standard header value from the specified connection request, or an empty string if no header value
     * has been specified for the request.
     *
     * @param headerName A {@code String} that represents the name of the header being requested.
     * @return A {@code String} that represents the header value, or {@code null} if there is no corresponding
     * header value for {@code headerName}.
     */
    private String getStandardHeaderValue(final String headerName) {
        final Header headerValue = requestBase.getFirstHeader(headerName);
        return headerValue == null ? Constants.EMPTY_STRING : headerValue.getValue();
    }

    /**
     * Computes a signature for the specified string using the HMAC-SHA256 algorithm.
     * Package-private because it is used to generate SAS signatures.
     *
     * @param stringToSign The UTF-8-encoded string to sign.
     * @return A {@code String} that contains the HMAC-SHA256-encoded signature.
     * @throws InvalidKeyException If the accountKey is not a valid Base64-encoded string.
     */
    String computeHmac256(final String stringToSign) throws InvalidKeyException {
        try {
            /*
             * We must get a new instance of the Mac calculator for each signature calculated because the instances are
             * not threadsafe and there is some suggestion online that they may not even be safe for reuse, so we use a
             * new one each time to be sure.
             */
            Mac hmacSha256 = Mac.getInstance("HmacSHA256");
            hmacSha256.init(new SecretKeySpec(this.accountKey, "HmacSHA256"));
            byte[] utf8Bytes = stringToSign.getBytes(Constants.UTF8_CHARSET);
            return Base64.getEncoder().encodeToString(hmacSha256.doFinal(utf8Bytes));
        } catch (final UnsupportedEncodingException | NoSuchAlgorithmException e) {
            throw new Error(e);
        }
    }

    public String buildAuthenticationSignature(HttpRequestBase request) {
        this.requestBase = request;
        Header dateHeader = requestBase.getFirstHeader(HeaderConstants.DATE);
        if (dateHeader == null) {
            throw new IllegalArgumentException("Header: " + HeaderConstants.DATE + "is mandatory.");
        }

        final String stringToSign = buildStringToSign();
        log.info("[buildAuthenticationSignature] stringToSign: \n", stringToSign);
        try {
            final String computedBase64Signature = computeHmac256(stringToSign);
            String signature = "SharedKey " + this.accountName + ":" + computedBase64Signature;

            return signature;
        } catch (Exception e) {
            log.error("[computeHmac256] {}", e);
            return null;
        }
    }
}
