package org.talend.components.workday.service;

import lombok.Data;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Data
@Service
public class AccessTokenService {

    @Service
    private AccessTokenProvider service;

    private WorkdayDataStore datastore;

    private transient Token token = null;

    public Token findToken() {
        if (this.token == null || this.isTokenTooOld()) {
            this.newToken();
        }
        return this.token;
    }

    private boolean isTokenTooOld() {
        return token.getExpireDate().isAfter(Instant.now().minus(30, ChronoUnit.SECONDS));
    }

    private synchronized void newToken() {
        this.token = service.getAccessToken(this.datastore);
    }
}
