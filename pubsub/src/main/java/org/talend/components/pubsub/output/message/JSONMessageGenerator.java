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
package org.talend.components.pubsub.output.message;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordConverters;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.spi.JsonbProvider;
import javax.json.spi.JsonProvider;

public class JSONMessageGenerator extends MessageGenerator {

    @Service
    private Jsonb jsonb;

    @Override
    public void init(PubSubDataSet dataset) {

    }

    @Override
    public PubsubMessage generateMessage(Record record) {
        RecordConverters converter = new RecordConverters();
        JsonBuilderFactory jsonBuilderFactory = Json.createBuilderFactory(null);
        JsonProvider jsonProvider = JsonProvider.provider();
        RecordBuilderFactory recordBuilderFactory = null;
        final JsonObject json = JsonObject.class.cast(converter.toType(new RecordConverters.MappingMetaRegistry(), record,
                JsonObject.class, () -> jsonBuilderFactory, () -> jsonProvider, () -> jsonb, () -> recordBuilderFactory));
        return PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(json.toString())).build();
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.JSON;
    }
}
