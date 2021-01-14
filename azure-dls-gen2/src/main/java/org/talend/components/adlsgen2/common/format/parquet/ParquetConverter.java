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
package org.talend.components.adlsgen2.common.format.parquet;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.components.adlsgen2.common.format.avro.AvroConfiguration;
import org.talend.components.adlsgen2.common.format.avro.AvroConverter;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ParquetConverter extends AvroConverter implements RecordConverter<GenericRecord>, Serializable {

    private RecordBuilderFactory recordBuilderFactory;

    public static ParquetConverter of(RecordBuilderFactory factory,
            final @Configuration("parquetConfiguration") AvroConfiguration configuration) {
        return new ParquetConverter(factory, configuration);
    }

    private ParquetConverter(RecordBuilderFactory factory,
            final @Configuration("parquetConfiguration") AvroConfiguration configuration) {
        super(factory, configuration);
    }

}
