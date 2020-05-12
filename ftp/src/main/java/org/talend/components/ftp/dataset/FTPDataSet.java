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
package org.talend.components.ftp.dataset;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.FormatConfiguration;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.service.FTPService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.BuiltInSuggestable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.function.Function;

@Data
@DataSet("FtpDataset")
@Icon(value = Icon.IconType.CUSTOM, custom = "ftp")
@GridLayout(names = GridLayout.FormType.MAIN, value = { @GridLayout.Row("datastore"), @GridLayout.Row({ "path" }),
        @GridLayout.Row("format"), @GridLayout.Row("csvConfiguration") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("csvConfiguration"), })
@Slf4j
public class FTPDataSet implements Serializable {

    @Option
    @Documentation("FTP datastore.")
    private FTPDataStore datastore;

    @Option
    @Documentation("Path to work in.")
    private String path;

    @Option
    @Documentation("Format of files")
    @DefaultValue("CSV")
    @Required
    private Format format = Format.CSV;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("Configuration for CSV format")
    private CSVConfiguration csvConfiguration = new CSVConfiguration();

    public enum Format {
        CSV(FTPDataSet::getCsvConfiguration, "csv");

        private Function<FTPDataSet, ContentFormat> contentFormatProvider;

        @Getter
        private String extension;

        private Format(Function<FTPDataSet, ContentFormat> contentFormatProvider, String extension) {
            this.contentFormatProvider = contentFormatProvider;
            this.extension = extension;
        }

        public ContentFormat getConfiguration(FTPDataSet dataset) {
            return contentFormatProvider.apply(dataset);
        }
    }

    public ContentFormat getFormatConfiguration() {
        return format.getConfiguration(this);
    }
}
