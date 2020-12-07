/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.runtime.output;

import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class BlobWriterFactory {

    public static BlobWriter getWriter(OutputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            JsonBuilderFactory jsonFactory, AdlsGen2Service service, AdlsActiveDirectoryService tokenProviderService)
            throws Exception {
        switch (configuration.getDataSet().getFormat()) {
        case CSV:
            return new CsvBlobWriter(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
        case AVRO:
            return new AvroBlobWriter(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
        case JSON:
            return new JsonBlobWriter(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
        case PARQUET:
            return new ParquetBlobWriter(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
        default:
            throw new IllegalArgumentException("Unsupported file format");
        }
    }

}
