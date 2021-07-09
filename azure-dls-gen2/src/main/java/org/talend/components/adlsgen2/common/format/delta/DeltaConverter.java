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
package org.talend.components.adlsgen2.common.format.delta;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

import static java.util.stream.Collectors.toList;

@Slf4j
public class DeltaConverter implements RecordConverter<RowRecord>, Serializable {

    private RecordBuilderFactory recordBuilderFactory;

    @Getter
    private Schema schema;

    private DeltaConverter(final RecordBuilderFactory factory) {
        recordBuilderFactory = factory;
        schema = null;
        log.debug("[DeltaConverter] format: schema: {}", schema);
    }

    public static DeltaConverter of(final RecordBuilderFactory factory) {
        return new DeltaConverter(factory);
    }

    @Override
    public Schema inferSchema(RowRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);

        StructType schema = record.getSchema();
        for (StructField field : schema.getFields()) {
            builder.withEntry(inferField(field));
        }
        return builder.build();
    }

    private static Map<Class, Schema.Type> deltaType2SchemaType = new HashMap<>();

    {
        deltaType2SchemaType.put(StringType.class, Schema.Type.STRING);
        deltaType2SchemaType.put(IntegerType.class, Schema.Type.INT);
        deltaType2SchemaType.put(LongType.class, Schema.Type.LONG);
        // use string for decimal for not missing precision?
        deltaType2SchemaType.put(DecimalType.class, Schema.Type.STRING);
        deltaType2SchemaType.put(DateType.class, Schema.Type.DATETIME);
        deltaType2SchemaType.put(ShortType.class, Schema.Type.INT);
        deltaType2SchemaType.put(ByteType.class, Schema.Type.INT);
        deltaType2SchemaType.put(DoubleType.class, Schema.Type.DOUBLE);
        deltaType2SchemaType.put(FloatType.class, Schema.Type.FLOAT);
        deltaType2SchemaType.put(BooleanType.class, Schema.Type.BOOLEAN);
        deltaType2SchemaType.put(ArrayType.class, Schema.Type.ARRAY);
        deltaType2SchemaType.put(BinaryType.class, Schema.Type.BYTES);

        // TODO make sure them
        deltaType2SchemaType.put(NullType.class, Schema.Type.STRING);
        deltaType2SchemaType.put(TimestampType.class, Schema.Type.DATETIME);

        deltaType2SchemaType.put(StructType.class, Schema.Type.RECORD);

        // TODO how to map map?
        // deltaType2SchemaType.put(MapType.class, Schema.Type.STRING);
    }

    private Schema.Entry inferField(StructField field) {
        Schema.Entry.Builder builder = recordBuilderFactory.newEntryBuilder();

        DataType type = field.getDataType();

        builder.withName(field.getName()).withNullable(field.isNullable());

        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            boolean containsNull = arrayType.containsNull();
            DataType elementType = arrayType.getElementType();
            builder.withType(Schema.Type.ARRAY);

            Schema.Type eType = deltaType2SchemaType.get(elementType.getClass());
            if (eType == null) {
                throw new RuntimeException("Not supported format:" + elementType.getTypeName());
            }

            Schema.Builder subBuilder = recordBuilderFactory.newSchemaBuilder(eType);

            if (elementType instanceof ArrayType) {
                // not support the Array<Array>
                throw new RuntimeException("Not supported array nested array direct");
            } else if (elementType instanceof StructType) {
                StructType structType = (StructType) elementType;
                Arrays.stream(structType.getFields()).map(this::inferField).forEach(subBuilder::withEntry);
                builder.withElementSchema(subBuilder.build());
            } else {
                builder.withElementSchema(subBuilder.build());
            }
        } else if (type instanceof MapType) {
            throw new RuntimeException("Not supported format:" + type.getTypeName());
        } else if (type instanceof StructType) {
            builder.withType(Schema.Type.RECORD);

            Schema.Builder subBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
            StructType structType = (StructType) type;
            Arrays.stream(structType.getFields()).map(this::inferField).forEach(subBuilder::withEntry);
            builder.withElementSchema(subBuilder.build());
        } else {
            Schema.Type stype = deltaType2SchemaType.get(type.getClass());
            if (stype == null) {
                // TODO should impossible execution here, only for safe
                throw new RuntimeException("Not supported format:" + type.getTypeName());
            }
            builder.withType(stype);
        }

        return builder.build();
    }

    @Override
    public Record toRecord(RowRecord rowRecord) {
        if (schema == null) {
            schema = inferSchema(rowRecord);
        }
        return rowRecordToRecord(rowRecord, rowRecord.getSchema().getFields(), recordBuilderFactory.newRecordBuilder(schema));
    }

    private Record rowRecordToRecord(RowRecord rowRecord, StructField[] fields, Record.Builder recordBuilder) {
        if (recordBuilder == null) {
            recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        }
        for (StructField field : fields) {
            Schema.Entry entry = inferField(field);
            if (field.getDataType() instanceof ArrayType) {
                buildArrayField(field, rowRecord, recordBuilder, entry);
            } else {
                if (!entry.isNullable() || !rowRecord.isNullAt(field.getName())) {
                    buildField(field, rowRecord, recordBuilder, entry);
                }
            }
        }
        return recordBuilder.build();
    }

    protected void buildArrayField(StructField field, RowRecord rowRecord, Record.Builder recordBuilder, Schema.Entry entry) {
        ArrayType arrayType = (ArrayType) field.getDataType();
        DataType elementType = arrayType.getElementType();
        String name = field.getName();
        boolean isNull = rowRecord.isNullAt(name);

        if (elementType instanceof StructType) {
            if (!isNull) {
                List<RowRecord> records = rowRecord.getList(name);
                Collection<Record> recs = records.stream().map(record -> rowRecordToRecord(record, record.getSchema().getFields(),
                        recordBuilderFactory.newRecordBuilder())).collect(toList());
                recordBuilder.withArray(entry, recs);
            }
        } else if (elementType instanceof DecimalType) {
            if (!isNull) {
                recordBuilder.withArray(entry, rowRecord.getList(name).stream()
                        .map(value -> value == null ? null : ((BigDecimal) value).toPlainString()).collect(toList()));
            }
        } else if (elementType instanceof ShortType) {
            // TODO check if need, and convert performance as too many wrap and unwrap
            if (!isNull) {
                recordBuilder.withArray(entry, rowRecord.getList(name).stream()
                        .map(value -> value == null ? null : ((Short) value).intValue()).collect(toList()));
            }
        } else if (elementType instanceof ByteType) {
            if (!isNull) {
                recordBuilder.withArray(entry, rowRecord.getList(name).stream()
                        .map(value -> value == null ? null : ((Byte) value).intValue()).collect(toList()));
            }
        } else if (elementType instanceof TimestampType) {
            if (!isNull) {
                recordBuilder.withArray(entry, rowRecord.getList(name).stream()
                        .map(value -> value == null ? null : ((Timestamp) value).getTime()).collect(toList()));
            }
        } else if (elementType instanceof MapType) {
            throw new RuntimeException("Not supported format:" + elementType.getTypeName());
        } else if (elementType instanceof ArrayType) {
            // not support the Array<Array>
            throw new RuntimeException("Not supported array nested array direct");
        } else if (elementType instanceof NullType) {
            // not set if that type, mean null?
        } else {
            // the types which match easy
            if (!isNull) {
                recordBuilder.withArray(entry, rowRecord.getList(name));
            }
        }
    }

    private void buildField(StructField field, RowRecord rowRecord, Record.Builder recordBuilder, Schema.Entry entry) {
        DataType dataType = field.getDataType();
        final String name = field.getName();
        final boolean isNullable = field.isNullable();
        final boolean isNull = rowRecord.isNullAt(name);

        if (dataType instanceof StringType) {
            recordBuilder.withString(entry, isNull ? null : rowRecord.getString(name));
        } else if (dataType instanceof IntegerType) {
            if (!isNull) {
                recordBuilder.withInt(entry, rowRecord.getInt(name));
            }
        } else if (dataType instanceof LongType) {
            if (!isNull) {
                recordBuilder.withLong(entry, rowRecord.getLong(name));
            }
        } else if (dataType instanceof DecimalType) {
            // TODO check it
            if (!isNull) {
                recordBuilder.withString(entry, rowRecord.getBigDecimal(name).toPlainString());
            }
        } else if (dataType instanceof DateType) {
            if (!isNull) {
                recordBuilder.withDateTime(entry, rowRecord.getDate(name));
            }
        } else if (dataType instanceof ShortType) {
            if (!isNull) {
                recordBuilder.withInt(entry, rowRecord.getShort(name));
            }
        } else if (dataType instanceof ByteType) {
            if (!isNull) {
                recordBuilder.withInt(entry, rowRecord.getByte(name));
            }
        } else if (dataType instanceof DoubleType) {
            if (!isNull) {
                recordBuilder.withDouble(entry, rowRecord.getDouble(name));
            }
        } else if (dataType instanceof FloatType) {
            if (!isNull) {
                recordBuilder.withFloat(entry, rowRecord.getFloat(name));
            }
        } else if (dataType instanceof BooleanType) {
            if (!isNull) {
                recordBuilder.withBoolean(entry, rowRecord.getBoolean(name));
            }
        } else if (dataType instanceof ArrayType) {
            buildArrayField(field, rowRecord, recordBuilder, entry);
        } else if (dataType instanceof BinaryType) {
            if (!isNull) {
                recordBuilder.withBytes(entry, rowRecord.getBinary(name));
            }
        } else if (dataType instanceof NullType) {
            // do nothing, not set, that mean null, TODO make sure it
        } else if (dataType instanceof TimestampType) {
            // TODO make sure it
            if (!isNull) {
                recordBuilder.withTimestamp(entry, rowRecord.getTimestamp(name).getTime());
            }
        } else if (dataType instanceof StructType) {
            if (!isNull) {
                recordBuilder.withRecord(entry,
                        rowRecordToRecord(rowRecord, rowRecord.getSchema().getFields(), recordBuilderFactory.newRecordBuilder()));
            }
        } else if (dataType instanceof MapType) {
            // TODO how to map this type?
            // use Record type with two fields: key, value? not sure right, as map also mean a collection of (key, value), a
            // record no that mean
            throw new RuntimeException("Not supported format:" + dataType.getTypeName());
        }
    }

    @Override
    public RowRecord fromRecord(Record record) {
        throw new UnsupportedOperationException("#fromRecord()");
    }

}
