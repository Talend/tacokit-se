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
package org.talend.components.common.formats;

import org.talend.components.common.formats.csv.CSVFormatOptions;
import org.talend.components.common.formats.csv.CSVFieldDelimiter;
import org.talend.components.common.formats.csv.CSVRecordDelimiter;
import org.talend.components.common.formats.excel.ExcelFormatOptions;

public class FormatUtils {

    public static String getUsedEncodingValue(CSVFormatOptions csvFormat) {

        return csvFormat.getEncoding() == Encoding.OTHER ? csvFormat.getCustomEncoding()
                : csvFormat.getEncoding().getEncodingCharsetValue();
    }

    public static String getUsedEncodingValue(ExcelFormatOptions excelFormat) {

        return excelFormat.getEncoding() == Encoding.OTHER ? excelFormat.getCustomEncoding()
                : excelFormat.getEncoding().getEncodingCharsetValue();
    }

    public static char getFieldDelimiterValue(CSVFormatOptions config) {
        return config.getFieldDelimiter() == CSVFieldDelimiter.OTHER ? config.getCustomFieldDelimiter().charAt(0)
                : config.getFieldDelimiter().getDelimiterValue();
    }

    public static String getRecordDelimiterValue(CSVFormatOptions config) {
        return config.getRecordDelimiter() == CSVRecordDelimiter.OTHER ? config.getCustomRecordDelimiter()
                : config.getRecordDelimiter().getDelimiterValue();
    }
}
