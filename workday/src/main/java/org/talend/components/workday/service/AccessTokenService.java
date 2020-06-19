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
package org.talend.components.workday.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.cache.LocalCache;
import java.util.Optional;

@Service
public class AccessTokenService {

    @Service
    private AccessTokenProvider service;

    @Service
    private LocalCache tokenCache;

    public Token findToken(WorkdayDataStore datastore) {
        return this.tokenCache.computeIfAbsent(Token.class, datastore.getClientId() + "-" + datastore.getEndpoint(),
                this::isTokenTooOld, () -> this.newToken(datastore));
    }

    private Token newToken(WorkdayDataStore datastore) {
        return service.getAccessToken(datastore);
    }

    private boolean isTokenTooOld(LocalCache.Element element) {
        return Optional.ofNullable(element).map(LocalCache.Element::getValue) // element value => token
                .filter(Token.class::isInstance) // should be true
                .map(Token.class::cast) //
                .map(Token::isTooOld) // test expiry date.
                .orElse(true);
    }

}
