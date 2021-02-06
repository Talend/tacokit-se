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

import java.util.Arrays;

public class BytesValidator extends Validator<byte[]> {

    @Override
    public boolean validate(Config.Condition condition, String expected, byte[] value, Record record) {
        final byte[] bexpected = Util.hexStringToByteArray(expected);

        switch (condition) {
        case EQUALS:
            return Arrays.equals(bexpected, value);
        case CONTAINS:
            final String sbytes = new String(value);
            final String sexpected = new String(bexpected);
            return sbytes.contains(sexpected);
        default:
            throw new UnsupportedOperationException("Unspported String condition : " + condition);
        }
    }

}
