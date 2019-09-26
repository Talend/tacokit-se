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

import static java.util.Optional.ofNullable;
import static org.talend.sdk.component.api.configuration.action.BuiltInSuggestable.Name.INCOMING_SCHEMA_ENTRY_NAMES;
import static org.talend.sdk.component.api.record.Schema.Type.RECORD;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

import java.io.Serializable;

import org.talend.components.preparation.service.PreparationService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.BuiltInSuggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
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
@Processor(name = "Concat")
@Documentation("Enable to concatenate two columns.")
public class Concat implements Serializable {

    private final Configuration configuration;

    private final PreparationService service;

    private final RecordBuilderFactory recordBuilderFactory;

    @ElementListener
    public Record onNext(final Record input) {
        return input.getSchema().getEntries().stream()
                .collect(service.toRecord(toOutputSchema(input), input,
                        (entry, builder) -> !entry.getName().equals(configuration.getOutputColumnName()),
                        (builder, ignored) -> builder.withString(configuration.getOutputColumnName(),
                                ofNullable(configuration.getPrefix()).orElse("")
                                        + input.getOptionalString(configuration.getFirstColumn())
                                                .orElseGet(configuration::getFirstDefault)
                                        + ofNullable(configuration.getSeparator()).orElse("")
                                        + input.getOptionalString(configuration.getSecondColumn())
                                                .orElseGet(configuration::getSecondDefault)
                                        + ofNullable(configuration.getSuffix()).orElse(""))));
    }

    private Schema toOutputSchema(final Record input) {
        final Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(RECORD);
        input.getSchema().getEntries().forEach(builder::withEntry);
        builder.withEntry(recordBuilderFactory.newEntryBuilder().withType(STRING).withName(configuration.getOutputColumnName())
                .withNullable(false).build());
        return builder.build();
    }

    @Data
    @GridLayout({ @GridLayout.Row({ "firstColumn", "secondColumn" }), @GridLayout.Row({ "firstDefault", "secondDefault" }),
            @GridLayout.Row({ "prefix", "suffix" }), @GridLayout.Row("separator"), @GridLayout.Row("outputColumnName") })
    public static class Configuration implements Serializable {

        @Option
        @Required
        @Documentation("Name of the first column to use in the concatenation. "
                + "It must be a primitive column (not a record or an array).")
        @BuiltInSuggestable(INCOMING_SCHEMA_ENTRY_NAMES)
        private String firstColumn;

        @Option
        @Required
        @Documentation("Name of the second column to use in the concatenation. "
                + "It must be a primitive column (not a record or an array).")
        @BuiltInSuggestable(INCOMING_SCHEMA_ENTRY_NAMES)
        private String secondColumn;

        @Option
        @Documentation("Prefix for the concatenation.")
        private String prefix = "";

        @Option
        @Documentation("Suffix for the concatenation.")
        private String suffix = "";

        @Option
        @Documentation("Separator between both columns.")
        private String separator;

        @Option
        @Documentation("Value to use when the first column is missing or null.")
        private String firstDefault = "";

        @Option
        @Documentation("Value to use when the second column is missing or null.")
        private String secondDefault = "";

        @Option
        @Required
        @Documentation("Name of the output column.")
        private String outputColumnName = "concatenation";
    }
}
