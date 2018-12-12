/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package org.talend.components.salesforce.commons;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.talend.daikon.avro.converter.AvroConverter;
import org.talend.daikon.avro.converter.IndexedRecordConverter;

public class BulkResultAdapterFactory implements IndexedRecordConverter<BulkResult, IndexedRecord> {

    private Schema schema;

    private String names[];

    /** The cached AvroConverter objects for the fields of this record. */
    @SuppressWarnings("rawtypes")
    protected transient AvroConverter[] fieldConverter;

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Class<BulkResult> getDatumClass() {
        return BulkResult.class;
    }

    @Override
    public BulkResult convertToDatum(IndexedRecord indexedRecord) {
        throw new UnmodifiableAdapterException();
    }

    @Override
    public IndexedRecord convertToAvro(BulkResult result) {
        return new ResultIndexedRecord(result);
    }

    @Override
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    private class ResultIndexedRecord implements IndexedRecord {

        private final BulkResult value;

        public ResultIndexedRecord(BulkResult value) {
            this.value = value;
        }

        @Override
        public void put(int i, Object o) {
            throw new UnmodifiableAdapterException();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object get(int i) {
            // Lazy initialization of the cached converter objects.
            if (names == null) {
                names = new String[getSchema().getFields().size()];
                fieldConverter = new AvroConverter[names.length];
                for (int j = 0; j < names.length; j++) {
                    Field f = getSchema().getFields().get(j);
                    names[j] = f.name();
                    fieldConverter[j] = SalesforceAvroRegistry.get().getConverterFromString(f);
                }
            }
            Object resultValue = value.getValue(names[i]);
            if (resultValue == null) {
                String columnName = names[i].substring(names[i].indexOf("_") + 1);
                resultValue = value.getValue(columnName);
            }
            if (resultValue != null && "".equals(resultValue)) {
                resultValue = null;
            }
            return fieldConverter[i].convertToAvro(resultValue);
        }

        @Override
        public Schema getSchema() {
            return BulkResultAdapterFactory.this.getSchema();
        }
    }
}
