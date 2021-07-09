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
package org.talend.components.common.stream.output.parquet.converter;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class SchemaWriterTest {

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @Test
    void convert() {

        final SchemaWriter schemaWriter = new SchemaWriter();

        final Schema schema1 = this.newSchema(Type.RECORD)
                .withEntry(this.newEntry("field1", Type.STRING).withNullable(false).build()).build();
        final MessageType messageType1 = schemaWriter.convert(schema1);

        Assertions.assertNotNull(messageType1);
        final org.apache.parquet.schema.Type field1 = messageType1.getType("!field1");
        Assertions.assertTrue(field1.isPrimitive());
        Assertions.assertEquals(Repetition.REQUIRED, field1.getRepetition());
        Assertions.assertEquals(PrimitiveTypeName.BINARY, field1.asPrimitiveType().getPrimitiveTypeName());
        Assertions.assertEquals(LogicalTypeAnnotation.stringType(), field1.asPrimitiveType().getLogicalTypeAnnotation());

        final Schema.Entry multiArrayInt = this.newEntry("multiArrayInt", Type.ARRAY)
                .withElementSchema(this.newSchema(Type.ARRAY).withElementSchema(this.newPrimitiveSchema(Type.INT)).build())
                .build();
        final Schema.Entry arrayOfRecord = this.newEntry("arrayOfRecord", Type.ARRAY)
                .withElementSchema(this.newSchema(Type.RECORD).withEntry(this.newEntry("time", Type.DATETIME).build())
                        .withEntry(this.newEntry("theLong", Type.LONG).build()).build())
                .build();
        final Schema complexSchema = this.newSchema(Type.RECORD).withEntry(multiArrayInt).withEntry(arrayOfRecord)
                .withEntry(newEntry("bytesField", Type.BYTES).build()).build();
        final MessageType complexMsg = schemaWriter.convert(complexSchema);
        Assertions.assertNotNull(complexMsg);

        final org.apache.parquet.schema.Type arrayInt = complexMsg.getType("!multiArrayInt");
        Assertions.assertFalse(arrayInt.isPrimitive());
        final org.apache.parquet.schema.Type arrayType = arrayInt.asGroupType().getType(0);
        Assertions.assertNotNull(arrayType);
        Assertions.assertFalse(arrayType.isPrimitive());
        Assertions.assertEquals(Repetition.REPEATED, arrayType.getRepetition());

        final org.apache.parquet.schema.Type elementType = arrayType.asGroupType().getType(0);
        Assertions.assertFalse(elementType.isPrimitive());

        final org.apache.parquet.schema.Type subList = elementType.asGroupType().getFields().get(0);
        Assertions.assertFalse(subList.isPrimitive());
        final org.apache.parquet.schema.Type subListElement = subList.asGroupType().getFields().get(0);

        Assertions.assertTrue(subListElement.isPrimitive());
        Assertions.assertEquals(PrimitiveTypeName.INT32, subListElement.asPrimitiveType().getPrimitiveTypeName());

        final org.apache.parquet.schema.Type arrayRecords = complexMsg.getType("!arrayOfRecord");
        Assertions.assertFalse(arrayRecords.isPrimitive());
    }

    private Schema.Builder newSchema(Schema.Type tckType) {
        return this.factory.newSchemaBuilder(tckType);
    }

    private Schema newPrimitiveSchema(Schema.Type tckType) {
        return this.newSchema(tckType).build();
    }

    private Schema.Entry.Builder newEntry(final String name, Schema.Type tckType) {
        return this.factory.newEntryBuilder().withType(tckType).withName(name);
    }
}