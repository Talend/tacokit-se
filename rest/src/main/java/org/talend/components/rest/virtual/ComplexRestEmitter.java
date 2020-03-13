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
package org.talend.components.rest.virtual;

import lombok.RequiredArgsConstructor;
import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.rest.processor.JSonExtractor;
import org.talend.components.rest.processor.JsonExtractorService;
import org.talend.components.rest.service.CompletePayload;
import org.talend.components.rest.service.RestService;
import org.talend.components.rest.source.RestEmitter;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.LinkedList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-rest")
@Emitter(name = "Input")
@Documentation("Http REST Input component")
@RequiredArgsConstructor
@Pollable(name = "Polling", resumeMethod = "resume")
public class ComplexRestEmitter implements Serializable {

    private final ComplexRestConfiguration configuration;

    private final RestService client;

    private final JsonExtractorService jsonExtractorService;

    private transient LinkedList<Object> items = null;

    private transient boolean resume = false;

    public void resume(Object configuration) {
        resume = true;
    }

    @Producer
    public Object next() {
        if (items == null || resume) {
            resume = false;

            items = new LinkedList<>();
            final RestEmitter delegateSource = new RestEmitter(configuration.getDataset().getRestConfiguration(), client);
            final CompletePayload global = delegateSource.next();

            final boolean isJson = !String.class.isInstance(global.getBody());
            final boolean isCompletePayload = configuration.getDataset().getRestConfiguration().getDataset().isCompletePayload();

            if (isJson) {
                processJsonResponse(global, isCompletePayload);
            } else {
                processOtherResponse(global, isCompletePayload);
            }
        }
        return items.isEmpty() ? null : items.removeFirst();
    }

    private void processOtherResponse(CompletePayload global, boolean isCompletePayload) {
        if (isCompletePayload) {
            items.add(global);
        } else {
            items.add(new StringBody((String) global.getBody()));
        }
    }

    private void processJsonResponse(CompletePayload global, boolean isCompletePayload) {
        final JsonStructure body = (JsonStructure) global.getBody();
        final JSonExtractor extractor = new JSonExtractor(configuration.getDataset().getJSonExtractorConfiguration(),
                jsonExtractorService);
        if (isCompletePayload) {
            items.add(new CompletePayload(global.getStatus(), global.getHeaders(), extractor.onElement(body)));
        } else {
            JsonValue jsonValue = extractor.onElement(body);

            if (jsonValue.getValueType() == JsonValue.ValueType.ARRAY) {
                items.addAll(jsonValue.asJsonArray());
            } else {
                items.add(jsonValue);
            }
        }
    }
}
