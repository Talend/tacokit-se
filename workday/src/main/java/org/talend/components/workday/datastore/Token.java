package org.talend.components.workday.datastore;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Instant;

@Getter
@RequiredArgsConstructor
public class Token {

    private final String accessToken;

    private final String tokenType;

    private final Instant expireDate;

    public String getAuthorizationHeaderValue() {
        return tokenType + ' ' + accessToken;
    }
}
