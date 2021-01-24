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

import org.talend.components.assertion.conf.Config;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.format.rawtext.ExtendedRawTextConfiguration;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordVisitor;

import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

public final class RecordValidator extends Validator<Record> {

    @Override
    public boolean validate(Config.Condition condition, String expected, Record value, Record record) {
        throw new RuntimeException("Record type is not supported and should. We do assertion on final values.");
    }

}
