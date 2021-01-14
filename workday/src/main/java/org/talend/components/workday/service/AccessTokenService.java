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
package org.talend.components.workday.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.Service;

@Service
public class AccessTokenService {

    @Service
    private AccessTokenProvider service;

    private transient Token token = null;

    public Token findToken(WorkdayDataStore datastore) {
        if (this.token == null || this.isTokenTooOld()) {
            this.newToken(datastore);
        }
        return this.token;
    }

    private boolean isTokenTooOld() {
        return token.getExpireDate().isBefore(Instant.now().plus(30, ChronoUnit.SECONDS));
    }

    private synchronized void newToken(WorkdayDataStore datastore) {
        this.token = service.getAccessToken(datastore);
    }
}
