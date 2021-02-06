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
import org.talend.sdk.component.api.record.Record;

public class StringValidator extends Validator<String> {

    @Override
    public boolean validate(final Config.Condition condition, final String expected, final String value, Record record) {
        switch (condition) {
        case EQUALS:
            return expected.equals(value);
        case CONTAINS:
            return value.contains(expected);
        default:
            throw new UnsupportedOperationException("Unspported String condition : " + condition);
        }
    }
}
