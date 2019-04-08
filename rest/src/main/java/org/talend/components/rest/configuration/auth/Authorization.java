package org.talend.components.rest.configuration.auth;

public interface Authorization {

    /**
     * @return The basic Authorization header value `Basic LFKFIGTBGKG`
     */
    String getAuthorizationHeader();

    enum AuthorizationType {
        NoAuth,
        Basic,
        Bearer,
        Oauth2,
    }

}
