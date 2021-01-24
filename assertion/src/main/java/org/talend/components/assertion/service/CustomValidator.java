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

import bsh.EvalError;
import bsh.Interpreter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.assertion.conf.Config;
import org.talend.components.common.text.Substitutor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointer;
import org.talend.sdk.component.api.record.RecordPointerFactory;

import java.util.Collection;
import java.util.function.UnaryOperator;

import static java.util.Optional.ofNullable;

@Slf4j
public class CustomValidator extends Validator {

    private final String DEFAULT_IMPORTS = "import org.talend.sdk.component.api.record.Record;\n"
            + "import org.talend.sdk.component.api.record.Schema;\n"
            + "import org.talend.sdk.component.api.record.Schema.Entry;\n"
            + "import org.talend.sdk.component.api.record.Schema.Type;\n"
            + "import org.talend.components.assertion.service.Util;";

    private final Interpreter interpreter;

    public CustomValidator() {
        super();

        interpreter = new Interpreter();
        try {
            interpreter.eval(DEFAULT_IMPORTS);
        } catch (EvalError e) {
            throw new RuntimeException("Cant process default import in beanshell for CustomValidator.", e);
        }

    }

    @Override
    boolean validate(Config.Condition condition, String code, Object value, Record record) {
        final Substitutor.KeyFinder parameterFinder = new Substitutor.KeyFinder("${", "}");
        final RecordDictionary dictionary = new RecordDictionary(record, recordPointerFactory, value.toString());
        final Substitutor substitutor = new Substitutor(parameterFinder, dictionary);

        final String substituted = substitutor.replace(code);
        log.info(AssertService.LOG_PREFIX + "CUSTOM validator substituted value to test : " + substituted);

        try {
            interpreter.eval("Boolean valid = " + substituted);
            Boolean valid = (Boolean) interpreter.get("valid");
            return valid;
        } catch (EvalError e) {
            throw new RuntimeException("Cant process default custom validation within beanshell for CustomValidator.", e);
        }
    }

    private static class RecordDictionary implements UnaryOperator<String> {

        private final Record record;

        private final RecordPointerFactory recordPointerFactory;

        private final String value;

        public RecordDictionary(Record record, RecordPointerFactory recordPointerFactory, String value) {
            this.record = record;
            this.recordPointerFactory = recordPointerFactory;
            this.value = value;
        }

        public String apply(String key) {
            if ("value".equals(key)) {
                return this.value;
            }

            final RecordPointer rp = recordPointerFactory.apply(key);
            try {
                final Object value = rp.getValue(record, Object.class);

                return ofNullable(value).filter(v -> !(Record.class.isInstance(v) || Collection.class.isInstance(v)))
                        .map(String::valueOf).orElse(null); // If other than null, then ':-' default syntax in place holder
                // is not taken into account
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }

    }
}
