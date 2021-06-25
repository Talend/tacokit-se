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
package org.talend.components.common.stream.output.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class TCKParquetWriterBuilder extends ParquetWriter.Builder<Record, TCKParquetWriterBuilder> {

    private Schema schema;

    public TCKParquetWriterBuilder(Path file) {
        super(file);
    }

    public TCKParquetWriterBuilder(OutputFile file) {
        super(file);
    }

    public TCKParquetWriterBuilder withSchema(Schema schema) {
        this.schema = schema;
        return this.self();
    }

    @Override
    protected TCKParquetWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<Record> getWriteSupport(Configuration conf) {
        return new TCKWriteSupport(this.schema);
    }

}
