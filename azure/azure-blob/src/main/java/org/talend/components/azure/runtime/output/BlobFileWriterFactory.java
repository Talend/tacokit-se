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
package org.talend.components.azure.runtime.output;

import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.runtime.output.excel.ExcelBlobFileWriter;
import org.talend.components.azure.service.AzureBlobComponentServices;

public class BlobFileWriterFactory {

    public static BlobFileWriter getWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices)
            throws Exception {
        switch (config.getDataset().getFileFormat()) {
        case CSV:
            return new CSVBlobFileWriter(config, connectionServices);
        case AVRO:
            return new AvroBlobFileWriter(config, connectionServices);
        case EXCEL:
            return new ExcelBlobFileWriter(config, connectionServices);
        case PARQUET:
            return new ParquetBlobFileWriter(config, connectionServices);
        default:
            throw new IllegalArgumentException("Unsupported file format");
        }
    }

}