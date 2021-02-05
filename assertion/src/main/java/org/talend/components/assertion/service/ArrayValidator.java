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
import org.talend.sdk.component.api.record.Schema;

public class ArrayValidator extends Validator {

    @Override
    public boolean validate(Config.Condition condition, String expected, Object value, Record record) {
        final int indexType = expected.indexOf(':');
        if (indexType < 1) {
            throw new RuntimeException("Array type not defined should be 'TYPE:[json array]' : " + expected);
        }
        String type = expected.substring(0, indexType);
        Class clazz = Util.getClassFromType(Schema.Type.valueOf(type));
        String jsonArray = expected.substring(indexType + 1);

        // switch (condition) {
        /*
         * case EQUALS:
         * return Arrays.equals(bexpected, bytes);
         * case CONTAINS:
         * final String sbytes = new String(bytes);
         * final String sexpected = new String(bexpected);
         * return sbytes.contains(sexpected);
         */
        /*
         * default:
         * throw new UnsupportedOperationException("Unspported String condition : " + condition);
         * }
         */

        return true;
    }

}
