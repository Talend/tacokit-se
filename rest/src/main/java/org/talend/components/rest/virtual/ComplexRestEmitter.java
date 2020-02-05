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
package org.talend.components.rest.virtual;

import lombok.RequiredArgsConstructor;
import org.talend.components.rest.processor.JSonExtractor;
import org.talend.components.rest.processor.JsonExtractorService;
import org.talend.components.rest.service.RestService;
import org.talend.components.rest.source.RestEmitter;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import java.io.Serializable;
import java.io.StringReader;
import java.util.LinkedList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-rest")
@Emitter(name = "Input")
@Documentation("Http REST Input component")
@RequiredArgsConstructor
public class ComplexRestEmitter implements Serializable {

    private final ComplexRestConfiguration configuration;

    private final RestService client;

    private final JsonReaderFactory jsonReaderFactory;

    private final JsonExtractorService jsonExtractorService;

    private transient LinkedList<Object> items = null;

    @Producer
    public Object next() {
        if (items == null) {
            items = new LinkedList<>();
            final RestEmitter delegateSource = new RestEmitter(configuration.getRestConfiguration(), client);
            final Record global = delegateSource.next();
            if (!configuration.isComputeBody()) {
                items.add(global);
            } else {
                final JsonObject body = getBodyFromSource(global);
                final JSonExtractor extractor = new JSonExtractor(configuration.getJSonExtractorConfiguration(),
                        jsonExtractorService);
                extractor.onElement(body, value -> items.add(value));
            }
        }
        return items.isEmpty() ? null : items.removeFirst();
    }

    private JsonObject getBodyFromSource(final Record record) {
        final String body = record.getString("body");
        try (final JsonReader reader = jsonReaderFactory.createReader(new StringReader(body))) {
            return reader.readObject();
        }
    }
}
