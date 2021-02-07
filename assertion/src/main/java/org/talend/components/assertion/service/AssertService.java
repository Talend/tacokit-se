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
package org.talend.components.assertion.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.assertion.conf.Config;
import org.talend.components.assertion.conf.Config.AssertEntry;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointer;
import org.talend.sdk.component.api.record.RecordPointerFactory;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.record.RecordService;
import org.talend.sdk.component.api.service.update.Update;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.stream.JsonParserFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@Data
public class AssertService {

    public final static String SUPPORTED_TYPES = "SUPPORTED_TYPES";

    public final static String ARRAY_SUPPORTED_TYPES = "ARRAY_SUPPORTED_TYPES";

    public final static String LOAD_CONFIG = "LOAD_CONFIG";

    public final static String LOG_PREFIX = "ASSERT - ";

    @Service
    private JsonReaderFactory jsonReaderFactory;

    @Service
    private JsonParserFactory jsonParserFactory;

    @Service
    private RecordPointerFactory recordPointerFactory;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private RecordService recordService;

    @Update(LOAD_CONFIG)
    public Config.AssertionConfig loadConfig(final Config config) throws Exception {
        InputStream configInputStream = new ByteArrayInputStream(
                config.getAssertionConfig().getJsonConfiguration().getJsonConfiguration().getBytes());
        final JsonReader reader = jsonReaderFactory.createReader(configInputStream);

        final List<Config.AssertEntry> assertions = new ArrayList<>();
        if (config.getAssertionConfig().getJsonConfiguration().isLoadFromRecord()) {
            final JsonToRecord jsonToRecord = new JsonToRecord(recordBuilderFactory, false);
            final Record record = jsonToRecord.toRecord(reader.readObject());
            final List<AssertEntry> entries = recordToAssertions(record);
            assertions.addAll(entries);
        } else {
            final List<AssertEntry> entries = reader.readArray().stream().map(e -> e.asJsonObject()).map(this::jsonToAssertEntry)
                    .collect(Collectors.toList());
            assertions.addAll(entries);
        }
        config.getAssertionConfig().setAssertions(assertions);
        return config.getAssertionConfig();
    }

    private List<AssertEntry> recordToAssertions(final Record record) {
        final List<AssertEntry> assertions = recordService.visit(new RecordToAssertionVisitor(), record);

        return assertions;
    }

    public Config.AssertEntry jsonToAssertEntry(final JsonObject o) {
        final String array_type = o.getString("array_type", "STRING");
        final String type = o.getString("type", "STRING");
        final String condition = o.getString("condition", "EQUALS");
        final String custom = o.getString("custom", "");

        return new Config.AssertEntry(o.getString("path"), Schema.Type.valueOf(type), Schema.Type.valueOf(array_type),
                Config.Condition.valueOf(condition), o.getString("value"), custom, o.getString("message"));
    }

    @Suggestions(AssertService.SUPPORTED_TYPES)
    public SuggestionValues getArraySupportedTypes() {
        final List<Item> supportedTypes = _getSupportedTypes(Collections.emptyList());
        return new SuggestionValues(true, supportedTypes);
    }

    @Suggestions(AssertService.ARRAY_SUPPORTED_TYPES)
    public SuggestionValues getSupportedTypes() {
        final List<Item> supportedTypes = _getSupportedTypes(Arrays.asList(Type.ARRAY));
        return new SuggestionValues(true, supportedTypes);
    }

    private List<Item> _getSupportedTypes(final List<Type> excluded) {
        return Arrays.asList(Type.ARRAY, Type.STRING, Type.BYTES, Type.INT, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN,
                Type.DATETIME).stream().filter(t -> !excluded.contains(t)).sorted(new Comparator<Type>() {

                    @Override
                    public int compare(Type o1, Type o2) {
                        return o1.name().compareTo(o2.name());
                    }
                }).map(t -> new Item(t.name(), t.name())).collect(Collectors.toList());
    }

    public List<String> validate(final Config config, final Record record) {
        return validateAssertionsOnRecord(config.getAssertionConfig().getAssertions(), record, config.getDateFormat());
    }

    private List<String> validateAssertionsOnRecord(final List<Config.AssertEntry> assertions, final Record record,
            String dateFormat) {
        List<String> errors = new ArrayList<>();

        for (Config.AssertEntry asrt : assertions) {
            final Optional<String> s = validateOneAssertionOnrecord(asrt, record, dateFormat);
            s.ifPresent(e -> errors.add(e));
        }

        return errors;
    }

    private Optional<String> validateOneAssertionOnrecord(final Config.AssertEntry asrt, final Record record, String dateFormat) {

        final Validator validator = ValidatorFactory.createInstance(asrt, dateFormat);
        validator.setRecordBuilderFactory(recordBuilderFactory);
        validator.setRecordService(recordService);
        validator.setRecordPointerFactory(recordPointerFactory);
        validator.setJsonReaderFactory(jsonReaderFactory);
        validator.setJsonParserFactory(jsonParserFactory);

        final RecordPointer pointer = recordPointerFactory.apply(asrt.getPath());
        try {
            final Object value = pointer.getValue(record, Util.getClassFromType(asrt.getType()));

            String expected = asrt.getValue();
            if (asrt.getCondition() == Config.Condition.CUSTOM) {
                expected = asrt.getCustom();
            }

            final boolean validate = validator.validate(asrt.getCondition(), expected, value, record);
            String err = this.buildErrorMsg(validate, asrt, value);
            return Optional.ofNullable(err);
        } catch (IllegalArgumentException | ClassCastException e) {
            return Optional.ofNullable("Can't find '" + asrt.getType() + "' value for path '" + asrt.getPath() + "'");
        }
    }

    private String buildErrorMsg(boolean validate, Config.AssertEntry asrt, Object value) {
        if (validate) {
            log.info(AssertService.LOG_PREFIX + asrt + " with retrieved value '" + value + "' : SUCCESSFUL");
            return null;
        }

        return asrt + "\nRetrieved value was : " + value;

    }

}
