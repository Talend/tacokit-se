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
package org.talend.components.netsuite.source;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Supplier;

import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.service.Messages;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

/**
 * Responsible for translating of input NetSuite record to {@code IndexedRecord} according to schema.
 */
public class NsObjectInputTransducer extends NsObjectTransducer {

    private final RecordBuilderFactory recordBuilderFactory;

    /** Descriptor of NetSuite data model object. */
    private TypeDesc typeDesc;

    public NsObjectInputTransducer(BasicMetaData basicMetaData, Messages i18n, RecordBuilderFactory recordBuilderFactory,
            Schema schema, TypeDesc typeDesc, String apiVersion) {
        super(basicMetaData, i18n, apiVersion, schema);
        this.recordBuilderFactory = recordBuilderFactory;
        this.typeDesc = typeDesc;
    }

    /**
     * Translate NetSuite data model object to {@code IndexedRecord}.
     *
     * @return indexed record
     */
    public Record read(Supplier<Object> supplier) {
        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();
        Record.Builder builder = recordBuilderFactory.newRecordBuilder(schema);
        Map<String, Object> mapView = getMapView(supplier.get(), schema, typeDesc);

        for (Entry entry : schema.getEntries()) {
            String nsFieldName = Beans.toInitialLower(entry.getName());
            FieldDesc fieldDesc = fieldMap.get(nsFieldName);
            if (fieldDesc == null) {
                continue;
            }

            Object value = readField(mapView, fieldDesc);
            if (value == null) {
                continue;
            }

            if (fieldDesc.getRecordValueType() == Boolean.class) {
                builder.withBoolean(entry, (Boolean) value);
            } else if (fieldDesc.getRecordValueType() == Double.class) {
                builder.withDouble(entry, (Double) value);
            } else if (fieldDesc.getRecordValueType() == Integer.class) {
                builder.withInt(entry, (Integer) value);
            } else if (fieldDesc.getRecordValueType() == Long.class) {
                builder.withLong(entry, (Long) value);
            } else if (fieldDesc.getRecordValueType() == ZonedDateTime.class) {
                builder.withDateTime(entry, (ZonedDateTime) value);
            } else {
                builder.withString(entry, (String) value);
            }
        }

        return builder.build();
    }
}
