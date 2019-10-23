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
package org.talend.components.workday.input;

import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.Serializable;
import java.util.Iterator;

@Version(1)
@Icon(value = Icon.IconType.BURGER) // FIXME : find a real icon.
@Emitter(family = "Workday", name = "WQL")
@Documentation("Component to extract data from workday ERP via Workday Query Language")
public class WQLProducer implements Serializable {

    private static final long serialVersionUID = 2693235150546844805L;

    private final WQLConfiguration config;

    private transient final WorkdayReaderService reader;

    private transient Iterator<JsonObject> jsonIterator = null;

    public WQLProducer(@Option("configuration") WQLConfiguration config, WorkdayReaderService reader) {
        this.config = config;
        this.reader = reader;
    }

    @Producer
    public JsonObject next() {
        if (this.jsonIterator == null) {
            JsonObject obj = reader.find(this.config.getDataSet(), this.config.getDataSet().extractQueryParam());
            if (obj != null) {
                // {"error":"invalid request: WQL error.","errors":[{"error":"Field: accountCurrency.id is
                // invalid","field":"accountCurrency.id","location":"Error in select clause"}]}
                final String error = obj.getString("error", null);
                if (error != null) {
                    final JsonArray errors = obj.getJsonArray("errors");
                    throw new WorkdayException(error + " : " + errors.toString());
                }

                final JsonArray data = obj.getJsonArray("data");
                if (data != null && !data.isEmpty()) {
                    this.jsonIterator = data.stream().map(JsonObject.class::cast).iterator();
                }
            }
        }
        if (this.jsonIterator == null || !this.jsonIterator.hasNext()) {
            return null;
        }
        return this.jsonIterator.next();
    }
}