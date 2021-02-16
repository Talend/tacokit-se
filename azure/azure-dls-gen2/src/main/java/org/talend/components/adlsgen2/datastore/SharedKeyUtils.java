/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.datastore;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.talend.components.common.Constants;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;

import lombok.extern.slf4j.Slf4j;

/**
 * SharedKeyCredentials are a means of signing and authenticating storage requests. The key can be obtained from the
 * Azure portal. This factory will create policies which take care of all the details of creating strings to sign,
 * signing them, and setting the Authentication header. While this is a common way of authenticating with the service,
 * recommended practice is using {@link TokenCredentials}.
 *
 *
 * How you construct the signature string depends on which service and version you are authorizing against and which authorization
 * scheme you are using. When constructing the signature string, keep in mind the following:
 *
 * - The VERB portion of the string is the HTTP verb, such as GET or PUT, and must be uppercase.
 *
 * - For Shared Key authorization for the Blob, Queue, and File services, each header included in the signature string
 * may appear only once. If any header is duplicated, the service returns status code 400 (Bad Request).
 *
 * - The values of all standard HTTP headers must be included in the string in the order shown in the signature
 * format, without the header names. These headers may be empty if they are not being specified as part of the request; in that
 * case, only the new-line character is required.
 *
 * - If the x-ms-date header is specified, you may ignore the Date header, regardless of whether it is specified on
 * the request, and simply specify an empty line for the Date portion of the signature string. In this case, follow the
 * instructions in the Constructing the CanonicalizedHeaders Element section for adding the x-ms-date header.
 *
 * - It is acceptable to specify both x-ms-date and Date; in this case, the service uses the value of x-ms-date.
 *
 * - If the x-ms-date header is not specified, specify the Date header in the signature string, without including the
 * header name.
 *
 * - All new-line characters (\n) shown are required within the signature string.
 *
 * - The signature string includes canonicalized headers and canonicalized resource strings. Canonicalizing these
 * strings puts them into a standard format that is recognized by Azure Storage. For detailed information on constructing the
 * CanonicalizedHeaders and CanonicalizedResource strings that make up part of the signature string, see the appropriate sections
 * later in this topic.
 *
 *
 */
@Slf4j
public final class SharedKeyUtils {

    private final String accountName;

    private final byte[] accountKey;

    /**
     * Initializes a new instance of SharedKeyCredentials contains an account's name and its primary or secondary
     * accountKey.
     *
     * @param accountName The account name associated with the request.
     * @param accountKey The account access key used to authenticate the request.
     * @throws InvalidKeyException Thrown when the accountKey is ill-formatted.
     */
    public SharedKeyUtils(String accountName, String accountKey) {
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
    private String buildStringToSign(URL url, String method, Map<String, String> headers) {
        String contentLength = getStandardHeaderValue(headers, Constants.HeaderConstants.CONTENT_LENGTH);
        contentLength = contentLength.equals("") ? Constants.EMPTY_STRING : contentLength;

        return String.join("\n", //
                method, //
                getStandardHeaderValue(headers, Constants.HeaderConstants.CONTENT_ENCODING), //
                getStandardHeaderValue(headers, Constants.HeaderConstants.CONTENT_LANGUAGE), //
                contentLength, //
                getStandardHeaderValue(headers, Constants.HeaderConstants.CONTENT_MD5), //
                getStandardHeaderValue(headers, Constants.HeaderConstants.CONTENT_TYPE), //
                // x-ms-date header exists, so don't sign date header
                Constants.EMPTY_STRING, //
                getStandardHeaderValue(headers, Constants.HeaderConstants.IF_MODIFIED_SINCE), //
                getStandardHeaderValue(headers, Constants.HeaderConstants.IF_MATCH), //
                getStandardHeaderValue(headers, Constants.HeaderConstants.IF_NONE_MATCH), //
                getStandardHeaderValue(headers, Constants.HeaderConstants.IF_UNMODIFIED_SINCE), //
                getStandardHeaderValue(headers, Constants.HeaderConstants.RANGE), //
                getAdditionalXmsHeaders(headers), //
                getCanonicalizedResource(url));
    }

    private String getAdditionalXmsHeaders(final Map<String, String> headers) {
        // Add only headers that begin with 'x-ms-'
        final ArrayList<String> xmsHeaderNameArray = new ArrayList<>();
        for (String header : headers.keySet()) {
            String lowerCaseHeader = header.toLowerCase(Locale.ROOT);
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
            canonicalizedHeaders.append(headers.get(key));
        }
        return canonicalizedHeaders.toString();
    }

    /**
     * Canonicalized the resource to sign.
     *
     * @param requestURL A {@code java.net.URL} of the request.
     * @return The canonicalized resource to sign.
     */
    private String getCanonicalizedResource(URL requestURL) {
        // Resource path
        final StringBuilder canonicalizedResource = new StringBuilder("/");
        canonicalizedResource.append(this.accountName);
        // Note that AbsolutePath starts with a '/'.
        if (requestURL.getPath().length() > 0) {
            canonicalizedResource.append(requestURL.getPath().replaceAll("\\s", "%20"));
        } else {
            canonicalizedResource.append('/');
        }
        // check for no query params and return
        if (requestURL.getQuery() == null) {
            return canonicalizedResource.toString();
        }
        // The URL object's query field doesn't include the '?'. The QueryStringDecoder expects it.
        QueryStringDecoder queryDecoder = new QueryStringDecoder("?" + requestURL.getQuery());
        Map<String, List<String>> queryParams = queryDecoder.getParameters();
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
     * @param headers An object that represents the headers for the request.
     * @param headerName A {@code String} that represents the name of the header being requested.
     * @return A {@code String} that represents the header value, or {@code null} if there is no corresponding
     * header value for {@code headerName}.
     */
    private String getStandardHeaderValue(final Map<String, String> headers, final String headerName) {
        final String headerValue = headers.get(headerName);

        return headerValue == null ? Constants.EMPTY_STRING : headerValue;
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
        log.debug("[computeHmac256] stringToSign:\n-->\n{}\n<--", stringToSign);
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
            throw new AdlsGen2RuntimeException(e.getMessage());
        }
    }

    public String buildAuthenticationSignature(URL url, String method, Map<String, String> headers) {
        final String stringToSign = buildStringToSign(url, method, headers);
        try {
            final String computedBase64Signature = computeHmac256(stringToSign);
            return String.format("SharedKey %s:%s", this.accountName, computedBase64Signature);
        } catch (Exception e) {
            log.error("[computeHmac256] {}", e.getMessage());
            return null;
        }
    }
}
