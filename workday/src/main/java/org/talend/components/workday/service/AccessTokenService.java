package org.talend.components.workday.service;

import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

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
        return token.getExpireDate().isAfter(Instant.now().minus(30, ChronoUnit.SECONDS));
    }

    private synchronized void newToken(WorkdayDataStore datastore) {
        this.token = service.getAccessToken(datastore);
    }
}
