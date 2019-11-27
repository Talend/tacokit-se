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
package org.talend.components.magentocms.helpers;

import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerAuthenticationToken;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;

@Service
public class AuthorizationHelper {

    @Service
    private AuthorizationHandlerAuthenticationToken authorizationHandlerAuthenticationToken;

    @Service
    private AuthorizationHandlerLoginPassword authorizationHandlerLoginPassword;

    public String getAuthorization(MagentoDataStore magentoDataStore)
            throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        AuthorizationHandler authenticationHandler = getAuthHandler(magentoDataStore.getAuthenticationType());
        return authenticationHandler.getAuthorization(magentoDataStore);
    }

    private AuthorizationHandler getAuthHandler(AuthenticationType authenticationType) throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            throw new UnsupportedOperationException("Incorrect usage of OAuth1 authentication handler");
        } else if (authenticationType == AuthenticationType.AUTHENTICATION_TOKEN) {
            return authorizationHandlerAuthenticationToken;
        } else if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return authorizationHandlerLoginPassword;
        }
        throw new UnknownAuthenticationTypeException();
    }
}
