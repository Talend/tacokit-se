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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.assertion.conf.Config;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.record.RecordService;
import org.talend.sdk.component.api.service.record.RecordVisitor;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.swing.text.html.Option;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

@WithComponents("org.talend.components.assertion")
class AssertServiceTest {

    @Injected
    private BaseComponentsHandler handler;

    @Service
    AssertService assertService;

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    RecordService recordService;

    @BeforeEach
    public void beforeEach() {
        // Inject needed services
        handler.injectServices(this);

    }

    @Test
    public void testJsonPointerFound() {
        final Record nested = this.recordBuilderFactory.newRecordBuilder().withString("a_nested_string", "aaa1")
                .withInt("a_nested_int", 2234).build();
        final Record record = this.recordBuilderFactory.newRecordBuilder().withString("a_string", "aaa0").withInt("a_int", 1234)
                .withRecord("a_record", nested).build();

        Config conf = new Config();

        Config.AssertEntry check_a_string = new Config.AssertEntry("/a_string", Schema.Type.STRING, Config.Condition.EQUALS,
                "aaa0", "", "Check a 1st level string attribute.\"");
        conf.addAssertEntry(check_a_string);

        Config.AssertEntry check_a_nested_string = new Config.AssertEntry("/a_record/a_nested_string", Schema.Type.STRING,
                Config.Condition.EQUALS, "aaa1", "", "Check a 1st level string attribute.");
        conf.addAssertEntry(check_a_nested_string);

        Config.AssertEntry check_a_nested_int = new Config.AssertEntry("/a_record/a_nested_int", Schema.Type.INT,
                Config.Condition.EQUALS, "2234", "", "Check a 2st level int nested attribute.");
        conf.addAssertEntry(check_a_nested_string);

        final List<String> validate = assertService.validate(conf, record);
        assertEquals(0, validate.size());
    }

    @Test
    public void testJsonPointerNotFound() {
        final Record nested = this.recordBuilderFactory.newRecordBuilder().withString("a_nested_string", "aaa1")
                .withInt("a_nested_int", 2234).build();
        final Record record = this.recordBuilderFactory.newRecordBuilder().withString("a_string", "aaa0").withInt("a_int", 1234)
                .withRecord("a_record", nested).build();

        Config conf = new Config();

        Config.AssertEntry check_a_string = new Config.AssertEntry("/a_stringX", Schema.Type.STRING, Config.Condition.EQUALS,
                "aaa0", "", "Check a 1st level string attribute.");
        conf.addAssertEntry(check_a_string);

        Config.AssertEntry check_a_nested_string = new Config.AssertEntry("/a_record/a_nested_stringX", Schema.Type.STRING,
                Config.Condition.EQUALS, "aaa1", "", "Check a 1st level string attribute.");
        conf.addAssertEntry(check_a_nested_string);

        Config.AssertEntry check_a_nested_int = new Config.AssertEntry("/a_record/a_nested_int", Schema.Type.STRING,
                Config.Condition.EQUALS, "2234", "", "Check a 2st level int nested attribute.");
        conf.addAssertEntry(check_a_nested_string);

        final List<String> validate = assertService.validate(conf, record);
        assertEquals(3, validate.size());
    }

    @Test
    public void testCustomValidator() {
        final Record nested = this.recordBuilderFactory.newRecordBuilder().withString("a_nested_string", "aaa1")
                .withInt("a_nested_int", 2234).build();
        final Record record = this.recordBuilderFactory.newRecordBuilder().withString("a_string", "aaa1").withInt("a_int", 1234)
                .withRecord("a_record", nested).build();

        Config conf = new Config();

        Config.AssertEntry check_a_string = new Config.AssertEntry("/a_string", Schema.Type.STRING, Config.Condition.CUSTOM, "",
                "\"${/a_record/a_nested_string}\".equals(\"${value}\")", "Check a 1st level string attribute.");
        conf.addAssertEntry(check_a_string);

        final List<String> validate = assertService.validate(conf, record);
        assertEquals(0, validate.size());
    }

    @Test
    public void testGenerateConf() {
        final Record nested = this.recordBuilderFactory.newRecordBuilder().withString("a_nested_string", "aaa1")
                .withInt("a_nested_int", 2234).build();
        final Record record = this.recordBuilderFactory.newRecordBuilder().withString("a_string", "aaa1").withInt("a_int", 1234)
                .withRecord("a_record", nested).withDouble("a_double", 123.123d).withDateTime("a_dt", new Date()).build();

        final List<Config.AssertEntry> assertions = recordService.visit(new AssertConfRecordVisitor(recordService, ""), record);
        assertEquals(11, assertions.size());
    }

    public final static class AssertConfRecordVisitor implements RecordVisitor<List<Config.AssertEntry>> {

        private String currentPath;

        private RecordService recordService;

        private List<Config.AssertEntry> config = new ArrayList<>();

        public AssertConfRecordVisitor(final RecordService recordService, final String currentPath) {
            this.currentPath = currentPath;
            this.recordService = recordService;
        }

        @Override
        public List<Config.AssertEntry> get() {
            return config;
        }

        @Override
        public List<Config.AssertEntry> apply(final List<Config.AssertEntry> t1, final List<Config.AssertEntry> t2) {
            t1.addAll(t2);
            return t1;
        }

        @Override
        public RecordVisitor<List<Config.AssertEntry>> onRecord(final Schema.Entry entry, final Optional<Record> record) {
            return new AssertConfRecordVisitor(recordService, currentPath + "/" + entry.getName());
        }

        @Override
        public void onString(final Schema.Entry entry, final Optional<String> string) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), string));
        }

        @Override
        public void onInt(final Schema.Entry entry, final OptionalInt optionalInt) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), Optional.ofNullable(optionalInt.orElseGet(null))));
        }

        @Override
        public void onLong(final Schema.Entry entry, final OptionalLong optionalLong) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), Optional.ofNullable(optionalLong.orElseGet(null))));
        }

        @Override
        public void onFloat(final Schema.Entry entry, final OptionalDouble optionalFloat) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), Optional.ofNullable(optionalFloat.orElseGet(null))));
        }

        @Override
        public void onDouble(final Schema.Entry entry, final OptionalDouble optionalDouble) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), Optional.ofNullable(optionalDouble.orElseGet(null))));
        }

        @Override
        public void onBoolean(final Schema.Entry entry, final Optional<Boolean> optionalBoolean) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), optionalBoolean));
        }

        @Override
        public void onDatetime(final Schema.Entry entry, final Optional<ZonedDateTime> dateTime) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), dateTime));
        }

        @Override
        public void onBytes(final Schema.Entry entry, final Optional<byte[]> bytes) {
            config.add(buildAssertEntry(entry.getName(), entry.getType(), bytes));
        }

        /*
         * @Override
         * public RecordVisitor<T> onRecord(final Schema.Entry entry, final Optional<Record> record) {
         * return this;
         * }
         */

        /*
         * default void onIntArray(final Schema.Entry entry, final Optional<Collection<Integer>> array) {
         * }
         * 
         * default void onLongArray(final Schema.Entry entry, final Optional<Collection<Long>> array) {
         * }
         * 
         * default void onFloatArray(final Schema.Entry entry, final Optional<Collection<Float>> array) {
         * }
         * 
         * default void onDoubleArray(final Schema.Entry entry, final Optional<Collection<Double>> array) {
         * }
         * 
         * default void onBooleanArray(final Schema.Entry entry, final Optional<Collection<Boolean>> array) {
         * }
         * 
         * default void onStringArray(final Schema.Entry entry, final Optional<Collection<String>> array) {
         * }
         * 
         * default void onDatetimeArray(final Schema.Entry entry, final Optional<Collection<ZonedDateTime>> array) {
         * }
         * 
         * default void onBytesArray(final Schema.Entry entry, final Optional<Collection<byte[]>> array) {
         * }
         * 
         * default RecordVisitor<T> onRecordArray(final Schema.Entry entry, final Optional<Collection<Record>> array) {
         * return this;
         * }
         */

        private Config.AssertEntry buildAssertEntry(final String name, Schema.Type type, Optional value) {
            String path = currentPath + "/" + name;
            String msg = "'" + path + "' should be equals to '" + value.get() + "'.";
            return new Config.AssertEntry(path, type, value.isPresent() ? Config.Condition.EQUALS : Config.Condition.IS_NULL,
                    value.isPresent() ? String.valueOf(value.get()) : "", "", msg);
        }
    }

}