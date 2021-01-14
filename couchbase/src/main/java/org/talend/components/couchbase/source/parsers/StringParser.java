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
package org.talend.components.couchbase.source.parsers;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.StringDocument;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Slf4j
public class StringParser implements DocumentParser {

    private static final transient Logger LOG = LoggerFactory.getLogger(StringParser.class);

    private final Schema schemaStringDocument;

    private final RecordBuilderFactory builderFactory;

    public StringParser(RecordBuilderFactory builderFactory) {
        this.builderFactory = builderFactory;
        schemaStringDocument = builderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(builderFactory.newEntryBuilder().withName("id").withType(Schema.Type.STRING).build())
                .withEntry(builderFactory.newEntryBuilder().withName("content").withType(Schema.Type.STRING).build()).build();
    }

    @Override
    public Record parse(Bucket bucket, String id) {
        StringDocument doc;
        try {
            doc = bucket.get(id, StringDocument.class);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
        String data = doc.content();

        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schemaStringDocument);
        recordBuilder.withString("id", id);
        recordBuilder.withString("content", data);
        return recordBuilder.build();
    }

}
