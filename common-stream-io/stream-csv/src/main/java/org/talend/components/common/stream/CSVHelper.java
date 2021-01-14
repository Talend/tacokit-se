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
package org.talend.components.common.stream;

import org.apache.commons.csv.CSVFormat;
import org.talend.components.common.stream.format.csv.CSVConfiguration;

public class CSVHelper {

    public static CSVFormat getCsvFormat(CSVConfiguration config) {
        return CSVFormat.newFormat(config.findFieldSeparator()) //
                .withQuote(config.getQuotedValue()) //
                .withEscape(config.getEscape()) //
                .withRecordSeparator(config.getLineConfiguration().getLineSeparator()); //
    }

}
