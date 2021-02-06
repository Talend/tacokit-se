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

}