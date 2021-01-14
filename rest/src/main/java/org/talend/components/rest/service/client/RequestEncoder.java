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
package org.talend.components.rest.service.client;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.rest.service.I18n;
import org.talend.sdk.component.api.service.http.Encoder;

@Slf4j
public class RequestEncoder implements Encoder {

    private final I18n i18n;

    public RequestEncoder(final I18n i18n) {
        this.i18n = i18n;
    }

    @Override
    public byte[] encode(final Object value) {
        if (value == null) {
            // If encoder return null, the TCK http client throws a NPE
            return new byte[0];
        }
        if (!Body.class.isInstance(value)) {
            throw new IllegalArgumentException(i18n.badRequestBody(value.getClass().getName()));
        }

        Body body = Body.class.cast(value);

        byte[] content = body.getContent();

        log.debug(i18n.bodyContentLength(content.length));

        return content;
    }

}
