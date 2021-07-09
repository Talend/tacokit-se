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
package org.talend.components.common.stream.input.parquet;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.talend.sdk.component.api.record.Record;

public class TCKParquetReader {

    public static TCKParquetReader.Builder builder(InputFile file, final ReadSupport<Record> recordReadSupport) {
        return new TCKParquetReader.Builder(file, recordReadSupport);
    }

    public static class Builder extends ParquetReader.Builder<Record> {

        private final ReadSupport<Record> recordReadSupport;

        private Builder(final InputFile file, final ReadSupport<Record> recordReadSupport) {
            super(file);
            this.recordReadSupport = recordReadSupport;
        }

        @Override
        protected ReadSupport<Record> getReadSupport() {
            return this.recordReadSupport;
        }
    }
}
