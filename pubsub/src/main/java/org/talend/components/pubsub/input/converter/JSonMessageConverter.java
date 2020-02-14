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
package org.talend.components.pubsub.input.converter;

import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.runtime.record.RecordConverters;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.bind.Jsonb;
import javax.json.bind.spi.JsonbProvider;
import javax.json.stream.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class JSonMessageConverter extends MessageConverter {

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.JSON;
    }

    @Override
    public JsonObject convertMessage(PubsubMessage message) {
        InputStream in = new ByteArrayInputStream(getMessageContentAsBytes(message));
        JsonParser parser = Json.createParser(in);
        JsonObject jsonObject = parser.getObject();
        return jsonObject;
    }
}
