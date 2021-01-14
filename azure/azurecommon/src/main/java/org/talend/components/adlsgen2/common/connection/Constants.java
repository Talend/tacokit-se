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
package org.talend.components.adlsgen2.common.connection;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Constants {

    public static final String DFS_URL = "https://%s.dfs.core.windows.net";

    public static final String DFS_DEFAULT_ENDPOINT_SUFFIX = "dfs.core.windows.net";

    public static final String TOKEN_URL = "https://login.microsoftonline.com";

    public static final String TOKEN_FORM = "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=https://storage.azure.com/.default";

    public static final DateTimeFormatter RFC1123GMTDateFormatter = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ROOT).withZone(ZoneId.of("GMT"));

    public static final DateTimeFormatter ISO8601UTCDateFormatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT).withZone(ZoneId.of("UTC"));

    /**
     * Stores a reference to the UTC time zone.
     */
    public static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    /**
     * Stores a reference to the date/time pattern with the greatest precision Java.util.Date is capable of expressing.
     */
    private static final String MAX_PRECISION_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    /**
     * Stores a reference to the ISO8601 date/time pattern.
     */
    private static final String ISO8601_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * Stores a reference to the ISO8601 date/time pattern.
     */
    private static final String ISO8601_PATTERN_NO_SECONDS = "yyyy-MM-dd'T'HH:mm'Z'";

    /**
     * The length of a datestring that matches the MAX_PRECISION_PATTERN.
     */
    private static final int MAX_PRECISION_DATESTRING_LENGTH = MAX_PRECISION_PATTERN.replaceAll("'", "").length();

    /**
     * The master Microsoft Azure Storage header prefix.
     */
    public static final String PREFIX_FOR_STORAGE_HEADER = "x-ms-";

    /**
     * Constant representing a kilobyte (Non-SI version).
     */
    public static final int KB = 1024;

    /**
     * Constant representing a megabyte (Non-SI version).
     */
    public static final int MB = 1024 * KB;

    /**
     * An empty {@code String} to use for comparison.
     */
    public static final String EMPTY_STRING = "";

    /**
     * Specifies HTTP.
     */
    public static final String HTTP = "http";

    /**
     * Specifies HTTPS.
     */
    public static final String HTTPS = "https";

    /**
     * Specifies both HTTPS and HTTP.
     */
    public static final String HTTPS_HTTP = "https,http";

    /**
     * The default type for content-type and accept.
     */
    public static final String UTF8_CHARSET = "UTF-8";

    /**
     * The query parameter for snapshots.
     */
    public static final String SNAPSHOT_QUERY_PARAMETER = "snapshot";

    /**
     * The word redacted.
     */
    public static final String REDACTED = "REDACTED";

    /**
     * The default amount of parallelism for TransferManager operations.
     */
    // We chose this to match Go, which followed AWS' default.
    public static final int TRANSFER_MANAGER_DEFAULT_PARALLELISM = 5;

    public static final int HTTP_RESPONSE_CODE_200_OK = 200;

    public static final int HTTP_RESPONSE_CODE_201_CREATED = 201;

    public static final int HTTP_RESPONSE_CODE_202_ACCEPTED = 202;

    public static final String ATTR_FILESYSTEMS = "filesystems";

    public static final String ATTR_ACCOUNT = "account";

    public static final String ATTR_NAME = "name";

    public static final String ATTR_ACCESS_TOKEN = "access_token";

    public static final String ATTR_FILESYSTEM = "filesystem";

    public static final String ATTR_PATHS = "paths";

    public static final String ATTR_ACTION_APPEND = "append";

    public static final String ATTR_ACTION_FLUSH = "flush";

    public static final String ATTR_FILE = "file";

    /**
     * Private Default Ctor
     */
    private Constants() {
        // Private to prevent construction.
    }

    /**
     * Defines constants for use with HTTP headers.
     */
    public static final class HeaderConstants {

        public static final String DFS_CONTENT_TYPE = "application/json; charset=utf-8";

        /**
         * The Authorization header.
         */
        public static final String AUTHORIZATION = "Authorization";

        /**
         * The format string for specifying ranges with only begin offset.
         */
        public static final String BEGIN_RANGE_HEADER_FORMAT = "bytes=%d-";

        /**
         * The header that indicates the client request ID.
         */
        public static final String CLIENT_REQUEST_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "client-request-id";

        /**
         * The ContentEncoding header.
         */
        public static final String CONTENT_ENCODING = "Content-Encoding";

        /**
         * The ContentLangauge header.
         */
        public static final String CONTENT_LANGUAGE = "Content-Language";

        /**
         * The ContentLength header.
         */
        public static final String CONTENT_LENGTH = "Content-Length";

        /**
         * The ContentMD5 header.
         */
        public static final String CONTENT_MD5 = "Content-MD5";

        /**
         * The ContentType header.
         */
        public static final String CONTENT_TYPE = "Content-Type";

        /**
         * The header that specifies the date.
         */
        public static final String DATE = PREFIX_FOR_STORAGE_HEADER + "date";

        /**
         * The header that specifies the error code on unsuccessful responses.
         */
        public static final String ERROR_CODE = PREFIX_FOR_STORAGE_HEADER + "error-code";

        /**
         * The IfMatch header.
         */
        public static final String IF_MATCH = "If-Match";

        /**
         * The IfModifiedSince header.
         */
        public static final String IF_MODIFIED_SINCE = "If-Modified-Since";

        /**
         * The IfNoneMatch header.
         */
        public static final String IF_NONE_MATCH = "If-None-Match";

        /**
         * The IfUnmodifiedSince header.
         */
        public static final String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";

        /**
         * The Range header.
         */
        public static final String RANGE = "Range";

        /**
         * The format string for specifying ranges.
         */
        public static final String RANGE_HEADER_FORMAT = "bytes=%d-%d";

        /**
         * The copy source header.
         */
        public static final String COPY_SOURCE = "x-ms-copy-source";

        /**
         * The version header.
         */
        public static final String VERSION = "x-ms-version";

        /**
         * The current storage version header value.
         */
        // public static final String TARGET_STORAGE_VERSION = "2018-03-28";
        public static final String TARGET_STORAGE_VERSION = "2018-11-09";

        /**
         * The UserAgent header.
         */
        public static final String USER_AGENT = "User-Agent";

        /**
         * Specifies the value to use for UserAgent header.
         */
        public static final String USER_AGENT_PREFIX = "Azure-Storage";

        /**
         * Specifies the value to use for UserAgent header.
         */
        public static final String USER_AGENT_VERSION = "10.5.0";

        public static final String HEADER_X_MS_ERROR_CODE = "x-ms-error-code";

        public static final String AUTH_BEARER = "Bearer %s";

        public static final String AUTH_SHARED_ACCESS_SIGNATURE = "SharedAccessSignature %s";

        public static final String TRANSFERT_ENCODING = "Transfer-Encoding";

        public static final String ACCEPT = "Accept";

        public static final String ACCEPT_DEFAULT = "application/json, */*";

        private HeaderConstants() {
            // Private to prevent construction.
        }
    }

}
