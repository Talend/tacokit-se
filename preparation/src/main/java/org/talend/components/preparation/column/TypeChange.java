/**
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.components.preparation.column;

import static org.talend.sdk.component.api.configuration.action.BuiltInSuggestable.Name.INCOMING_SCHEMA_ENTRY_NAMES;
import static org.talend.sdk.component.api.record.Schema.Type.RECORD;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;

import org.talend.components.preparation.configuration.NewColumn;
import org.talend.components.preparation.service.PreparationService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.BuiltInSuggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Version
@RequiredArgsConstructor
@Icon(Icon.IconType.STAR)
@Processor(name = "TypeChange")
@Documentation("Enable to change a column type.")
public class TypeChange implements Serializable {

    private final Configuration configuration;

    private final PreparationService service;

    private final RecordBuilderFactory recordBuilderFactory;

    @ElementListener
    public Record onNext(final Record input) {
        final String populatedColumn = configuration.getNewColumn().isCreate() ? configuration.getNewColumn().getName()
                : configuration.getColumn();
        return input.getSchema().getEntries().stream()
                .collect(service.toRecord(toOutputSchema(input, populatedColumn), input, (entry, builder) -> {
                    if (entry.getName().equals(configuration.getColumn())) {
                        configuration.getDesiredType().append(builder, entry, input, populatedColumn);
                        return !configuration.getNewColumn().isCreate()
                                || configuration.getNewColumn().getName().equals(configuration.getColumn());
                    }
                    return false;
                }, (builder, appended) -> {
                    if (!appended) {
                        configuration.getDesiredType().append(builder,
                                // fake a missing column to fallback on the default
                                recordBuilderFactory.newEntryBuilder().withType(STRING).withName(configuration.getColumn())
                                        .build(),
                                input, populatedColumn);
                    }
                }));
    }

    private Schema toOutputSchema(final Record input, final String populatedColumn) {
        final Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(RECORD);
        input.getSchema().getEntries().stream().filter(it -> !it.getName().equals(populatedColumn)).forEach(builder::withEntry);
        builder.withEntry(recordBuilderFactory.newEntryBuilder().withType(configuration.getDesiredType().getNativeType())
                .withName(populatedColumn).withNullable(false).build());
        return builder.build();
    }

    @Data
    @OptionsOrder({ "column", "desiredType", "newColumn" })
    public static class Configuration implements Serializable {

        @Option
        @Required
        @Documentation("Name of the column to change the type to.")
        @BuiltInSuggestable(INCOMING_SCHEMA_ENTRY_NAMES)
        private String column;

        @Option
        @Required
        @Documentation("Expected type in the output.")
        private Type desiredType;

        @Option
        @Documentation("Configuration to specify if the incoming column is replaced or leads to a new column creation.")
        private NewColumn newColumn = new NewColumn();

        @Getter
        @RequiredArgsConstructor
        enum Type {
            /*
             * not supported yet
             * RECORD(Schema.Type.RECORD),
             * ARRAY(Schema.Type.RECORD),
             */
            STRING(Schema.Type.RECORD) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case INT:
                        builder.withString(targetColumn, Integer.toString(source.getOptionalInt(entryName).orElse(0)));
                        break;
                    case LONG:
                        builder.withString(targetColumn, Long.toString(source.getOptionalLong(entryName).orElse(0)));
                        break;
                    case FLOAT:
                        builder.withString(targetColumn, Float.toString((float) source.getOptionalFloat(entryName).orElse(0.)));
                        break;
                    case DOUBLE:
                        builder.withString(targetColumn, Double.toString(source.getOptionalDouble(entryName).orElse(0.)));
                        break;
                    case BOOLEAN:
                        builder.withString(targetColumn, Boolean.toString(source.getOptionalBoolean(entryName).orElse(false)));
                        break;
                    case DATETIME:
                        builder.withString(targetColumn,
                                source.getOptionalDateTime(entryName).map(ZonedDateTime::toString).orElse(""));
                        break;
                    case BYTES:
                        builder.withString(targetColumn,
                                source.getOptionalBytes(entryName).map(v -> Base64.getEncoder().encodeToString(v)).orElse(""));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            BYTES(Schema.Type.BYTES) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case INT:
                        builder.withBytes(targetColumn,
                                Integer.toString(source.getOptionalInt(entryName).orElse(0)).getBytes(StandardCharsets.UTF_8));
                        break;
                    case LONG:
                        builder.withBytes(targetColumn,
                                Long.toString(source.getOptionalLong(entryName).orElse(0)).getBytes(StandardCharsets.UTF_8));
                        break;
                    case FLOAT:
                        builder.withBytes(targetColumn, Float.toString((float) source.getOptionalFloat(entryName).orElse(0))
                                .getBytes(StandardCharsets.UTF_8));
                        break;
                    case DOUBLE:
                        builder.withBytes(targetColumn,
                                Double.toString(source.getOptionalDouble(entryName).orElse(0)).getBytes(StandardCharsets.UTF_8));
                        break;
                    case BOOLEAN:
                        builder.withBytes(targetColumn, Boolean.toString(source.getOptionalBoolean(entryName).orElse(false))
                                .getBytes(StandardCharsets.UTF_8));
                        break;
                    case DATETIME:
                        builder.withBytes(targetColumn, source.getOptionalDateTime(entryName).map(ZonedDateTime::toString)
                                .orElse("").getBytes(StandardCharsets.UTF_8));
                        break;
                    case STRING:
                        builder.withBytes(targetColumn,
                                source.getOptionalString(entryName).orElse("").getBytes(StandardCharsets.UTF_8));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            INT(Schema.Type.INT) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case STRING:
                        builder.withInt(targetColumn, source.getOptionalString(entryName).map(Integer::parseInt).orElse(0));
                        break;
                    case LONG:
                        builder.withInt(targetColumn, (int) source.getOptionalLong(entryName).orElse(0L));
                        break;
                    case FLOAT:
                        builder.withInt(targetColumn, (int) source.getOptionalFloat(entryName).orElse(0));
                        break;
                    case DOUBLE:
                        builder.withInt(targetColumn, (int) source.getOptionalLong(entryName).orElse(0));
                        break;
                    case BOOLEAN:
                        builder.withInt(targetColumn, source.getOptionalBoolean(entryName).orElse(false) ? 1 : 0);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            LONG(Schema.Type.LONG) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case STRING:
                        builder.withDouble(targetColumn, source.getOptionalString(entryName).map(Double::parseDouble).orElse(0.));
                        break;
                    case INT:
                        builder.withDouble(targetColumn, source.getOptionalInt(entryName).orElse(0));
                        break;
                    case FLOAT:
                        builder.withDouble(targetColumn, source.getOptionalFloat(entryName).orElse(0));
                        break;
                    case DOUBLE:
                        builder.withDouble(targetColumn, source.getOptionalDouble(entryName).orElse(0));
                        break;
                    case BOOLEAN:
                        builder.withDouble(targetColumn, source.getOptionalBoolean(entryName).orElse(false) ? 1 : 0);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            FLOAT(Schema.Type.FLOAT) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case STRING:
                        builder.withFloat(targetColumn, source.getOptionalString(entryName).map(Float::parseFloat).orElse(0f));
                        break;
                    case INT:
                        builder.withFloat(targetColumn, source.getOptionalInt(entryName).orElse(0));
                        break;
                    case LONG:
                        builder.withFloat(targetColumn, source.getOptionalLong(entryName).orElse(0));
                        break;
                    case DOUBLE:
                        builder.withFloat(targetColumn, (float) source.getOptionalDouble(entryName).orElse(0));
                        break;
                    case BOOLEAN:
                        builder.withFloat(targetColumn, source.getOptionalBoolean(entryName).orElse(false) ? 1 : 0);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            DOUBLE(Schema.Type.DOUBLE) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case STRING:
                        builder.withDouble(targetColumn, source.getOptionalString(entryName).map(Double::parseDouble).orElse(0.));
                        break;
                    case INT:
                        builder.withDouble(targetColumn, source.getOptionalInt(entryName).orElse(0));
                        break;
                    case LONG:
                        builder.withDouble(targetColumn, source.getOptionalLong(entryName).orElse(0));
                        break;
                    case FLOAT:
                        builder.withDouble(targetColumn, source.getOptionalFloat(entryName).orElse(0));
                        break;
                    case BOOLEAN:
                        builder.withDouble(targetColumn, source.getOptionalBoolean(entryName).orElse(false) ? 1 : 0);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            BOOLEAN(Schema.Type.BOOLEAN) {

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    final String entryName = entry.getName();
                    switch (entry.getType()) {
                    case INT:
                        builder.withBoolean(targetColumn, source.getOptionalInt(entryName).orElse(0) != 0);
                        break;
                    case LONG:
                        builder.withBoolean(targetColumn, source.getOptionalLong(entryName).orElse(0) != 0);
                        break;
                    case FLOAT:
                        builder.withBoolean(targetColumn, source.getOptionalFloat(entryName).orElse(0) != 0);
                        break;
                    case DOUBLE:
                        builder.withBoolean(entryName, source.getOptionalDouble(entryName).orElse(0) != 0);
                        break;
                    case STRING:
                        builder.withBoolean(targetColumn,
                                source.getOptionalString(entryName).map(Boolean::parseBoolean).orElse(false));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            },
            DATETIME(Schema.Type.DATETIME) {

                private final ZonedDateTime defaultDate = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

                @Override
                public void append(final Record.Builder builder, final Schema.Entry entry, final Record source,
                        final String targetColumn) {
                    switch (entry.getType()) {
                    case STRING:
                        source.getOptionalString(targetColumn)
                                .map(v -> builder.withDateTime(entry.getName(), ZonedDateTime.parse(v)))
                                .orElseGet(() -> builder.withDateTime(entry.getName(), defaultDate));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported incoming type: " + entry);
                    }
                }
            };

            private final Schema.Type nativeType;

            public abstract void append(Record.Builder builder, Schema.Entry entry, Record source, String targetColumn);
        }
    }
}
