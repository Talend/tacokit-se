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
package org.talend.components.rest.source;

import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.CompletePayload;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.LinkedList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-rest")
@Emitter(name = "Input")
@Documentation("Http REST Input component")
@Pollable(name = "Polling", resumeMethod = "resume")
public class RestEmitter implements Serializable {

    private final RequestConfig config;

    private final RestService client;

    private transient LinkedList<Object> items;

    private boolean done;

    public void resume(Object configuration) {
        done = false;
    }

    public RestEmitter(@Option("configuration") final RequestConfig config, final RestService client) {
        this.config = config;
        this.client = client;
    }

    @Producer
    public Object next() {
        if (items == null) {
            items = new LinkedList<>();
        }

        if (items.isEmpty() && !done) {
            done = true;
            final CompletePayload completePayload = client.buildFixedRecord(client.execute(config));

            final boolean isCompletePayload = config.getDataset().isCompletePayload();
            final boolean isJson = !String.class.isInstance(completePayload.getBody());
            if (isJson) {
                processJsonResponse(completePayload, isCompletePayload);
            } else {
                processOtherResponse(completePayload, isCompletePayload);
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

    private void processJsonResponse(CompletePayload completePayload, boolean isCompletePayload) {
        if (isCompletePayload) {
            items.add(completePayload);
        } else {
            final JsonStructure body = (JsonStructure) completePayload.getBody();

            if (body.getValueType() == JsonValue.ValueType.ARRAY) {
                items.addAll(body.asJsonArray());
            } else {
                items.add(body);
            }
        }
    }

}
