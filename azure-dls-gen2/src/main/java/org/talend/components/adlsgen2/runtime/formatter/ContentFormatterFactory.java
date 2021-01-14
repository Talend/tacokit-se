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
package org.talend.components.adlsgen2.runtime.formatter;

import java.io.Serializable;

import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.I18n;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ContentFormatterFactory implements Serializable {

    public static ContentFormatter getFormatter(@Option("configuration") final OutputConfiguration configuration,
            final AdlsGen2Service service, final I18n i18n, final RecordBuilderFactory recordBuilderFactory,
            final JsonBuilderFactory jsonBuilderFactory) {

        switch (configuration.getDataSet().getFormat()) {
        case CSV:
            return new CsvContentFormatter(configuration, recordBuilderFactory);
        case AVRO:
            return new AvroContentFormatter(configuration, recordBuilderFactory);
        case JSON:
            return new JsonContentFormatter(configuration, recordBuilderFactory, jsonBuilderFactory);
        case PARQUET:
            return new ParquetContentFormatter(configuration, recordBuilderFactory);
        default:
            throw new IllegalArgumentException("Unsupported file format");
        }
    }
}
