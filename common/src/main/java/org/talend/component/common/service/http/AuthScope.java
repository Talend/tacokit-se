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
package org.talend.component.common.service.http;

import java.util.Locale;

import lombok.EqualsAndHashCode;

/**
 * {@code AuthScope} represents an authentication scope consisting of a host name,
 * a port number, a realm name and an authentication scheme name.
 * <p>
 * This class can also optionally contain a host of origin, if created in response
 * to authentication challenge from a specific host.
 * </p>
 * 
 * @since 4.0
 */

@EqualsAndHashCode(of = { "host", "port", "realm", "scheme" })
public class AuthScope {

    /**
     * The {@code null} value represents any host. In the future versions of
     * HttpClient the use of this parameter will be discontinued.
     */
    public static final String ANY_HOST = null;

    /**
     * The {@code -1} value represents any port.
     */
    public static final int ANY_PORT = -1;

    /**
     * The {@code null} value represents any realm.
     */
    public static final String ANY_REALM = null;

    /**
     * The {@code null} value represents any authentication scheme.
     */
    public static final String ANY_SCHEME = null;

    /**
     * Default scope matching any host, port, realm and authentication scheme.
     * In the future versions of HttpClient the use of this parameter will be
     * discontinued.
     */
    public static final AuthScope ANY = new AuthScope(ANY_HOST, ANY_PORT, ANY_REALM, ANY_SCHEME);

    /** The authentication scheme the credentials apply to. */
    private String scheme;

    /** The realm the credentials apply to. */
    private String realm;

    /** The host the credentials apply to. */
    private String host;

    /** The port the credentials apply to. */
    private int port;

    /** The original host, if known */
    // private DigestAuthContext origin;

    /**
     * Defines auth scope with the given {@code host}, {@code port}, {@code realm}, and
     * {@code schemeName}.
     *
     * @param host authentication host. May be {@link #ANY_HOST} if applies
     * to any host.
     * @param port authentication port. May be {@link #ANY_PORT} if applies
     * to any port of the host.
     * @param realm authentication realm. May be {@link #ANY_REALM} if applies
     * to any realm on the host.
     * @param schemeName authentication scheme. May be {@link #ANY_SCHEME} if applies
     * to any scheme supported by the host.
     */
    public AuthScope(final String host, final int port, final String realm, final String schemeName) {
        this.host = host == null ? ANY_HOST : host.toLowerCase(Locale.ROOT);
        this.port = port < 0 ? ANY_PORT : port;
        this.realm = realm == null ? ANY_REALM : realm;
        this.scheme = schemeName == null ? ANY_SCHEME : schemeName.toUpperCase(Locale.ROOT);
        // this.origin = null;
    }

    /**
     * Defines auth scope for a specific host of origin.
     *
     * @param origin host of origin
     * @param realm authentication realm. May be {@link #ANY_REALM} if applies
     * to any realm on the host.
     * @param schemeName authentication scheme. May be {@link #ANY_SCHEME} if applies
     * to any scheme supported by the host.
     *
     * @since 4.2
     */
    public AuthScope(final DigestAuthContext origin, final String realm, final String schemeName) {
        this.host = origin.getHost().toLowerCase(Locale.ROOT);
        this.port = origin.getPort() < 0 ? ANY_PORT : origin.getPort();
        this.realm = realm == null ? ANY_REALM : realm;
        this.scheme = schemeName == null ? ANY_SCHEME : schemeName.toUpperCase(Locale.ROOT);
        // this.origin = origin;
    }

    /**
     * Defines auth scope for a specific host of origin.
     *
     * @param origin host of origin
     *
     * @since 4.2
     */
    public AuthScope(final DigestAuthContext origin) {
        this(origin, ANY_REALM, ANY_SCHEME);
    }

    /**
     * Defines auth scope with the given {@code host}, {@code port} and {@code realm}.
     *
     * @param host authentication host. May be {@link #ANY_HOST} if applies
     * to any host.
     * @param port authentication port. May be {@link #ANY_PORT} if applies
     * to any port of the host.
     * @param realm authentication realm. May be {@link #ANY_REALM} if applies
     * to any realm on the host.
     */
    public AuthScope(final String host, final int port, final String realm) {
        this(host, port, realm, ANY_SCHEME);
    }

    /**
     * Defines auth scope with the given {@code host} and {@code port}.
     *
     * @param host authentication host. May be {@link #ANY_HOST} if applies
     * to any host.
     * @param port authentication port. May be {@link #ANY_PORT} if applies
     * to any port of the host.
     */
    public AuthScope(final String host, final int port) {
        this(host, port, ANY_REALM, ANY_SCHEME);
    }

    /**
     * Creates a copy of the given credentials scope.
     */
    public AuthScope(final AuthScope authscope) {
        super();
        this.host = authscope.getHost();
        this.port = authscope.getPort();
        this.realm = authscope.getRealm();
        this.scheme = authscope.getScheme();
        // this.origin = authscope.getOrigin();
    }

    /**
     * @return host of origin. If unknown returns @null,
     *
     * @since 4.4
     */
    /*
     * public HttpHost getOrigin() {
     * return this.origin;
     * }
     */

    /**
     * @return the host
     */
    public String getHost() {
        return this.host;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return this.port;
    }

    /**
     * @return the realm name
     */
    public String getRealm() {
        return this.realm;
    }

    /**
     * @return the scheme type
     */
    public String getScheme() {
        return this.scheme;
    }

    /**
     * Tests if the authentication scopes match.
     *
     * @return the match factor. Negative value signifies no match.
     * Non-negative signifies a match. The greater the returned value
     * the closer the match.
     */
    public int match(final AuthScope that) {
        int factor = 0;
        if (langUtilEquals(this.scheme, that.scheme)) {
            factor += 1;
        } else {
            if (this.scheme != ANY_SCHEME && that.scheme != ANY_SCHEME) {
                return -1;
            }
        }
        if (langUtilEquals(this.realm, that.realm)) {
            factor += 2;
        } else {
            if (this.realm != ANY_REALM && that.realm != ANY_REALM) {
                return -1;
            }
        }
        if (this.port == that.port) {
            factor += 4;
        } else {
            if (this.port != ANY_PORT && that.port != ANY_PORT) {
                return -1;
            }
        }
        if (langUtilEquals(this.host, that.host)) {
            factor += 8;
        } else {
            if (this.host != ANY_HOST && that.host != ANY_HOST) {
                return -1;
            }
        }
        return factor;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        if (this.scheme != null) {
            buffer.append(this.scheme.toUpperCase(Locale.ROOT));
            buffer.append(' ');
        }
        if (this.realm != null) {
            buffer.append('\'');
            buffer.append(this.realm);
            buffer.append('\'');
        } else {
            buffer.append("<any realm>");
        }
        if (this.host != null) {
            buffer.append('@');
            buffer.append(this.host);
            if (this.port >= 0) {
                buffer.append(':');
                buffer.append(this.port);
            }
        }
        return buffer.toString();
    }

    private static boolean langUtilEquals(final Object obj1, final Object obj2) {
        return obj1 == null ? obj2 == null : obj1.equals(obj2);
    }

}
