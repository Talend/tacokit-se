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

import org.talend.components.assertion.conf.Config;

import org.talend.sdk.component.api.record.Schema;

public class ValidatorFactory {

    private ValidatorFactory() {
    }

    public static Validator createInstance(final Config.AssertEntry asrt, String dateFormat) {

        final Config.Condition condition = asrt.getCondition();
        switch (condition) {
        case IS_NULL:
            return new NullValidator();
        case CUSTOM:
            return new CustomValidator();
        }

        final Schema.Type type = asrt.getType();
        switch (type) {
        case RECORD:
            return new RecordValidator();
        case ARRAY:
            return new ArrayValidator();
        case STRING:
            return new StringValidator();
        case BYTES:
            return new BytesValidator();
        case INT:
            return new IntegerValidator();
        case LONG:
            return new LongValidator();
        case FLOAT:
            return new DoubleValidator();
        case DOUBLE:
            return new DoubleValidator();
        case BOOLEAN:
            return new BooleanValidator();
        case DATETIME:
            final DateValidator dateValidator = new DateValidator();
            dateValidator.setFormat(dateFormat);
            return dateValidator;
        default:
            throw new UnsupportedOperationException("Type to validate not supported : " + type);
        }

    }

}
