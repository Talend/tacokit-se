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
package org.talend.components.common.service.http.digest;

import lombok.Data;
import org.talend.components.common.service.http.common.UserNamePassword;

@Data
public class DigestAuthContext {

    private String uri;

    private String method;

    private String host;

    private int port;

    private byte[] payload;

    private String digestAuthHeader;

    private UserNamePassword credentials;

    public DigestAuthContext(final String uri, final String method, final String host, final int port, final byte[] payload,
            final UserNamePassword credentials) {
        this.uri = uri;
        this.method = method;
        this.host = host;
        this.port = port;
        this.payload = payload;
        this.credentials = credentials;
    }

    public boolean hasPayload() {
        if (payload == null) {
            return false;
        }
        return payload.length > 0;
    }

}
