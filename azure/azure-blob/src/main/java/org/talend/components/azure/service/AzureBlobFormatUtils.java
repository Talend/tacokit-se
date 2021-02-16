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

import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.common.formats.FormatUtils;

public class AzureBlobFormatUtils {

    public static String getUsedEncodingValue(AzureBlobDataset dataset) {
        switch (dataset.getFileFormat()) {
        case CSV:
            return FormatUtils.getUsedEncodingValue(dataset.getCsvOptions());
        case EXCEL:
            return FormatUtils.getUsedEncodingValue(dataset.getExcelOptions());

        default:
            throw new IllegalStateException("Avro and parquet data format doesn't support custom encodings");
        }
    }
}
