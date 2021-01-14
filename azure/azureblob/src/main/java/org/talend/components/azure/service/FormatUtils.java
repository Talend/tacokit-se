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
package org.talend.components.azure.service;

import org.talend.components.azure.common.Encoding;
import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.common.csv.FieldDelimiter;
import org.talend.components.azure.common.csv.RecordDelimiter;
import org.talend.components.azure.dataset.AzureBlobDataset;

public class FormatUtils {

    public static String getUsedEncodingValue(AzureBlobDataset dataset) {
        switch (dataset.getFileFormat()) {
        case CSV:
            return dataset.getCsvOptions().getEncoding() == Encoding.OTHER ? dataset.getCsvOptions().getCustomEncoding()
                    : dataset.getCsvOptions().getEncoding().getEncodingValue();
        // FIXME uncomment it when excel will be ready to integrate
        /*
         * case EXCEL:
         * return dataset.getExcelOptions().getEncoding() == Encoding.OTHER ? dataset.getExcelOptions().getCustomEncoding()
         * : dataset.getExcelOptions().getEncoding().getEncodingValue();
         */
        default:
            throw new IllegalStateException("Avro and parquet data format doesn't support custom encodings");
        }
    }

    public static char getFieldDelimiterValue(CSVFormatOptions config) {
        return config.getFieldDelimiter() == FieldDelimiter.OTHER ? config.getCustomFieldDelimiter().charAt(0)
                : config.getFieldDelimiter().getDelimiterValue();
    }

    public static String getRecordDelimiterValue(CSVFormatOptions config) {
        return config.getRecordDelimiter() == RecordDelimiter.OTHER ? config.getCustomRecordDelimiter()
                : config.getRecordDelimiter().getDelimiterValue();
    }
}
