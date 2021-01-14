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
package org.talend.components.google.storage.input;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.google.storage.dataset.JsonAllConfiguration;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.exception.ComponentException.ErrorOrigin;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class JsonAllReaderSupplier implements RecordReaderSupplier {

    @Override
    public RecordReader getReader(RecordBuilderFactory factory, ContentFormat config, Object extraParameter) {
        if (!JsonAllConfiguration.class.isInstance(config)) {
            throw new ComponentException(ErrorOrigin.BACKEND, "try to get json-all-reader with other than json-all-config");
        }
        final JsonAllConfiguration jsonCfg = (JsonAllConfiguration) config;
        final JsonToRecord toRecord = new JsonToRecord(factory, jsonCfg.isForceDouble());
        return new JsonAllRecordReader(toRecord);
    }
}
