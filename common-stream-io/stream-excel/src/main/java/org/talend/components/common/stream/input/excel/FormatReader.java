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
package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.components.common.stream.format.excel.ExcelConfiguration.ExcelFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public interface FormatReader {

    Iterator<Record> read(InputStream input, ExcelConfiguration configuration) throws IOException;

    static FormatReader findReader(ExcelFormat format, RecordBuilderFactory factory) {
        if (format == ExcelFormat.EXCEL97 || format == ExcelFormat.EXCEL2007) {
            return new ExcelReader(factory);
        }
        return new HTMLReader(factory);
    }
}
