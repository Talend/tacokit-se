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
package org.talend.components.assertion.service;

import lombok.Data;
import org.talend.components.assertion.conf.Config;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointerFactory;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.record.RecordService;

import javax.json.JsonReaderFactory;
import javax.json.stream.JsonParserFactory;

@Data
public abstract class Validator<E> {

    public RecordPointerFactory recordPointerFactory;

    public RecordBuilderFactory recordBuilderFactory;

    public RecordService recordService;

    private JsonReaderFactory jsonReaderFactory;

    private JsonParserFactory jsonParserFactory;

    abstract boolean validate(final Config.Condition condition, final String expected, final E value, final Record record);

}
