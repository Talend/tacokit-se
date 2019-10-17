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
package org.talend.components.rest.service;

import org.talend.components.rest.configuration.RequestBody;
import org.talend.sdk.component.api.service.http.Encoder;

import static org.talend.components.common.service.http.UrlEncoder.queryEncode;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;

public class RequestEncoder implements Encoder {

    @Override
    public byte[] encode(final Object value) {
        if (value == null) {
            return null;
        }
        if (!RequestBody.class.isInstance(value)) {
            throw new IllegalStateException("Request body need to be of type " + RequestBody.class.getName());
        }

        RequestBody body = RequestBody.class.cast(value);

        return body.getType().getBytes(body);
    }

}
