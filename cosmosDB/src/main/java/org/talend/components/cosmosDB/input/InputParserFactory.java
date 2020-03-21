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
package org.talend.components.cosmosDB.input;

import com.microsoft.azure.documentdb.Document;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.cosmosDB.dataset.DocumentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.Iterator;

public class InputParserFactory {

    Iterator<Document> iterator;

    final RecordBuilderFactory builderFactory;

    DocumentType documentType;

    public InputParserFactory(final DocumentType documentType, final RecordBuilderFactory builderFactory,
            Iterator<Document> iterator) {
        this.documentType = documentType;
        this.iterator = iterator;
        this.builderFactory = builderFactory;
    }

    public IInputParser getInputParser() {
        if (DocumentType.TEXT == documentType) {
            return new StringParse();
        } else {
            return new JsonParse();
        }
    }

    interface IInputParser {

        Record get();
    }

    class StringParse implements IInputParser {

        private final Schema schemaStringDocument;

        private StringParse() {
            schemaStringDocument = builderFactory.newSchemaBuilder(Schema.Type.RECORD)
                    .withEntry(builderFactory.newEntryBuilder().withName("id").withType(Schema.Type.STRING).build())
                    .withEntry(builderFactory.newEntryBuilder().withName("content").withType(Schema.Type.STRING).build()).build();
        }

        @Override
        public Record get() {
            if (iterator.hasNext()) {
                Document next = iterator.next();
                String id = next.getId();
                final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schemaStringDocument);
                recordBuilder.withString("id", id);
                recordBuilder.withString("content", next.toJson());
                return recordBuilder.build();
            }
            return null;
        }
    }

    class JsonParse implements IInputParser {

        final JsonToRecord jsonToRecord;

        JsonParse() {
            this.jsonToRecord = new JsonToRecord(builderFactory);
        }

        @Override
        public Record get() {
            if (iterator.hasNext()) {
                Document next = iterator.next();
                JsonReader reader = Json.createReader(new StringReader(next.toJson()));
                JsonObject jsonObject = reader.readObject();
                jsonToRecord.toRecord(jsonObject);
                return jsonToRecord.toRecord(jsonObject);
            }
            return null;
        }
    }

}
