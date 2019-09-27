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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class represents an authentication challenge consisting of a auth scheme
 * and either a single parameter or a list of name / value pairs.
 *
 * @since 5.0
 */
public final class AuthChallenge {

    private final ChallengeType challengeType;

    private final String scheme;

    private final String value;

    private final List<NameValuePair> params;

    public AuthChallenge(final ChallengeType challengeType, final String scheme, final String value,
            final List<? extends NameValuePair> params) {
        super();
        this.challengeType = challengeType;
        this.scheme = scheme;
        this.value = value;
        this.params = params != null ? Collections.unmodifiableList(new ArrayList<>(params)) : null;
    }

    public AuthChallenge(final ChallengeType challengeType, final String scheme, final NameValuePair... params) {
        this(challengeType, scheme, null, Arrays.asList(params));
    }

    public ChallengeType getChallengeType() {
        return challengeType;
    }

    public String getScheme() {
        return scheme;
    }

    public String getValue() {
        return value;
    }

    public List<NameValuePair> getParams() {
        return params;
    }

    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append(scheme).append(" ");
        if (value != null) {
            buffer.append(value);
        } else if (params != null) {
            buffer.append(params);
        }
        return buffer.toString();
    }

}
