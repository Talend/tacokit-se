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
package org.talend.components.workday.input;

import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.json.JsonObject;
import java.io.Serializable;
import java.util.Iterator;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "WorkdayInput")
@Emitter(family = "Workday", name = "Input")
@Documentation("Component to extract data from workday ERP via Workday Query Language or Report As A Service")
public class WorkdayProducer implements Serializable {

    private static final long serialVersionUID = 2693235150546844805L;

    private final WorkdayConfiguration config;

    private final WorkdayReaderService reader;

    private transient Iterator<JsonObject> jsonIterator = null;

    public WorkdayProducer(@Option("configuration") WorkdayConfiguration config, WorkdayReaderService reader) {
        this.config = config;
        this.reader = reader;
    }

    @Producer
    public JsonObject next() {
        if (this.jsonIterator == null) {
            final WorkdayDataSet workdayds = this.config.getDataSet();
            JsonObject obj = reader.find(workdayds.getDatastore(), workdayds, workdayds.extractQueryParam());
            this.jsonIterator = reader.extractIterator(obj, workdayds.getMode().arrayName);
        }
        if (this.jsonIterator == null || !this.jsonIterator.hasNext()) {
            return null;
        }
        return this.jsonIterator.next();
    }
}