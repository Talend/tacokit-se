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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.talend.components.common.text.Substitutor;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.talend.components.common.service.http.UrlEncoder.queryEncode;

@Data
@AllArgsConstructor
public class Body {

    private final RequestBody conf;

    private final Substitutor substitutor;

    private final String charsetName;

    public Body(final RequestConfig config, final Substitutor substitutor) {
        this.conf = config.getDataset().getBody();
        this.substitutor = substitutor;

        if (config.getDataset().getHasHeaders()) {
            Map<String, List<String>> headers = config.headers().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.singletonList(e.getValue())));
            charsetName = RestService.getCharsetName(headers);
        } else {
            charsetName = null;
        }
    }

    public byte[] getContent() {
        switch (conf.getType()) {
        case FORM_DATA:
            return formDataStrategy();
        case X_WWW_FORM_URLENCODED:
            return xwwwformStrategy();
        default:
            return textStrategy();
        }
    }

    private byte[] xwwwformStrategy() {
        return Base64.getUrlEncoder().encode(
                encode(conf.getParams().stream().map(param -> param.getKey() + "=" + queryEncode(substitute(param.getValue())))
                        .collect(Collectors.joining("&"))));
    }

    private byte[] formDataStrategy() {
        return encode(conf.getParams().stream().map(param -> param.getKey() + "=" + queryEncode(substitute(param.getValue())))
                .collect(Collectors.joining("\n")));

    }

    private byte[] textStrategy() {
        return encode(substitute(Optional.ofNullable(conf.getTextContent()).orElse("")));
    }

    private byte[] encode(String s) {
        if (charsetName == null) {
            if (RestService.DEFAULT_ENCODING == null) {
                return s.getBytes();
            } else {
                return s.getBytes(Charset.forName(RestService.DEFAULT_ENCODING));
            }
        } else {
            return s.getBytes(Charset.forName(charsetName));
        }
    }

    private String substitute(final String value) {
        return !value.contains(this.substitutor.getPrefix()) ? value : this.substitutor.replace(value);
    }
}
