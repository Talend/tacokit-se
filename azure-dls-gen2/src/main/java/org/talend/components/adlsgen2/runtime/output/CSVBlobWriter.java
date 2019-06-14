/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
import org.talend.components.adlsgen2.runtime.formatter.CsvContentFormatter;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVBlobWriter extends BlobWriter {

    private final String EXT_CSV = ".csv";

    private final CsvContentFormatter formatter;

    public CSVBlobWriter(OutputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            JsonBuilderFactory jsonFactory, AdlsGen2Service service) throws Exception {
        super(configuration, recordBuilderFactory, jsonFactory, service);
        formatter = new CsvContentFormatter(configuration, recordBuilderFactory);
    }

    @Override
    public void generateFile() {
        generateFileWithExtension(EXT_CSV);
    }

    @Override
    public void flush() {
        if (getBatch().isEmpty()) {
            return;
        }
        byte[] contentBytes = formatter.feedContent(getBatch());
        uploadContent(contentBytes);
        getBatch().clear();
        currentItem.setBlobPath("");
    }

    // @Override
    // public void flush() {
    // if (getBatch().isEmpty()) {
    // return;
    // }
    // try {
    // String content = "";
    // if (csvConfiguration.isHeader()) {
    // content = appendHeader();
    // }
    // content += convertBatchToString();
    // byte[] contentBytes = content.getBytes(csvConfiguration.effectiveFileEncoding());
    // uploadContent(contentBytes);
    // getBatch().clear();
    // currentItem.setBlobPath("");
    // } catch (UnsupportedEncodingException e) {
    // throw new IllegalStateException(e);
    // }
    // }

    // private String appendHeader() {
    // if (getSchema() == null || getSchema().getEntries().size() == 0) {
    // return "";
    // }
    // StringBuilder headerBuilder = new StringBuilder();
    // headerBuilder.append(getSchema().getEntries().get(0).getName());
    // for (int i = 1; i < getSchema().getEntries().size(); i++) {
    // headerBuilder.append(csvConfiguration.effectiveFieldDelimiter()).append(getSchema().getEntries().get(i).getName());
    // }
    // headerBuilder.append(csvConfiguration.effectiveRecordSeparator());
    // return headerBuilder.toString();
    // }
    //
    // private String convertBatchToString() {
    // try {
    // StringWriter stringWriter = new StringWriter();
    // Iterator<Record> recordIterator = getBatch().iterator();
    // CSVFormat format = CsvConverter.of(recordBuilderFactory, csvConfiguration).getCsvFormat();
    // CSVPrinter printer = new CSVPrinter(stringWriter, format);
    // while (recordIterator.hasNext()) {
    // printer.printRecord(convertRecordToArray(recordIterator.next()));
    // }
    // printer.flush();
    // printer.close();
    //
    // return stringWriter.toString();
    // } catch (IOException e) {
    // throw new IllegalStateException(e);
    // }
    // }
    //
    // private Object[] convertRecordToArray(Record record) {
    // Object[] array = new Object[record.getSchema().getEntries().size()];
    // for (int i = 0; i < getSchema().getEntries().size(); i++) {
    // if (getSchema().getEntries().get(i).getType() == Schema.Type.DATETIME) {
    // array[i] = record.getDateTime(getSchema().getEntries().get(i).getName());
    // } else if (getSchema().getEntries().get(i).getType() == Schema.Type.BYTES) {
    // array[i] = Arrays.toString(record.getBytes(getSchema().getEntries().get(i).getName()));
    // } else {
    // array[i] = record.get(Object.class, getSchema().getEntries().get(i).getName());
    // }
    // }
    // return array;
    // }
}
