package org.talend.components.preparation.bool;

import static org.talend.sdk.component.api.configuration.action.BuiltInSuggestable.Name.INCOMING_SCHEMA_ENTRY_NAMES;
import static org.talend.sdk.component.api.record.Schema.Type.BOOLEAN;
import static org.talend.sdk.component.api.record.Schema.Type.RECORD;

import java.io.Serializable;

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
import lombok.RequiredArgsConstructor;

@Version
@RequiredArgsConstructor
@Icon(Icon.IconType.STAR)
@Processor(name = "Negate")
@Documentation("Enable to negate a column.")
public class Negate implements Serializable {

    private final Configuration configuration;

    private final PreparationService service;

    private final RecordBuilderFactory recordBuilderFactory;

    @ElementListener
    public Record onNext(final Record input) {
        return input.getOptionalBoolean(configuration.getColumn()).map(value -> remap(input, value))
                .orElseGet(() -> remap(input, configuration.isDefaultValue()));
    }

    private Record remap(final Record input, final boolean value) {
        final String populatedColumn = configuration.getNewColumn().isCreate() ? configuration.getNewColumn().getName()
                : configuration.getColumn();
        return input.getSchema().getEntries().stream()
                .collect(service.toRecord(toOutputSchema(input, populatedColumn), input, (entry, builder) -> {
                    if (entry.getName().equals(populatedColumn)) {
                        builder.withBoolean(entry, !value);
                        return !configuration.getNewColumn().isCreate()
                                || configuration.getNewColumn().getName().equals(configuration.getColumn());
                    }
                    return false;
                }, (builder, appended) -> {
                    if (!appended) {
                        builder.withBoolean(populatedColumn, input.getOptionalBoolean(configuration.getColumn()).map(it -> !it)
                                .orElse(configuration.isDefaultValue()));

                    }
                }));
    }

    private Schema toOutputSchema(final Record input, final String columnToCheck) {
        if (input.getSchema().getEntries().stream().noneMatch(it -> it.getName().equals(columnToCheck))) {
            final Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(RECORD);
            input.getSchema().getEntries().forEach(builder::withEntry);
            builder.withEntry(
                    recordBuilderFactory.newEntryBuilder().withType(BOOLEAN).withName(columnToCheck).withNullable(false).build());
            return builder.build();
        }
        return input.getSchema();
    }

    @Data
    @OptionsOrder({ "column", "newColumn", "defaultValue" })
    public static class Configuration implements Serializable {

        @Option
        @Required
        @Documentation("Name of the column to negate. It must be a boolean column.")
        @BuiltInSuggestable(INCOMING_SCHEMA_ENTRY_NAMES)
        private String column;

        @Option
        @Documentation("Configuration to add or not a new column in the output record.")
        private NewColumn newColumn = new NewColumn();

        @Option
        @Documentation("The value to use to populate the column when incoming data is absent.")
        private boolean defaultValue;
    }
}