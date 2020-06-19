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
package org.talend.components.ftp.output;

import lombok.Data;
import org.talend.components.ftp.dataset.FTPDataSet;
import org.talend.components.ftp.service.FTPConnectorConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row("dataSet"), @GridLayout.Row("limitBy"), @GridLayout.Row("recordsLimit"),
        @GridLayout.Row({ "sizeLimit", "sizeUnit" }), @GridLayout.Row("debug") })
@Documentation("Configuration for FTP sink")
public class FTPOutputConfiguration implements FTPConnectorConfiguration {

    @Option
    @Documentation("DataSet")
    private FTPDataSet dataSet;

    @Option
    @Documentation("Enable debug mode")
    private boolean debug;

    @Option
    @Required
    @Documentation("How to limit remote file size.")
    @DefaultValue("SIZE")
    private LimitBy limitBy = LimitBy.SIZE;

    @Option
    @DefaultValue("100")
    @ActiveIfs(operator = ActiveIfs.Operator.OR, value = { @ActiveIf(target = "limitBy", value = "RECORDS"),
            @ActiveIf(target = "limitBy", value = "SIZE_AND_RECORDS") })
    @Documentation("Max number of records per file.")
    private int recordsLimit = 100;

    @Option
    @DefaultValue("100")
    @ActiveIfs(operator = ActiveIfs.Operator.OR, value = { @ActiveIf(target = "limitBy", value = "SIZE"),
            @ActiveIf(target = "limitBy", value = "SIZE_AND_RECORDS") })
    @Documentation("Max file size.")
    private int sizeLimit = 100;

    @Option
    @DefaultValue("MB")
    @ActiveIfs(operator = ActiveIfs.Operator.OR, value = { @ActiveIf(target = "limitBy", value = "SIZE"),
            @ActiveIf(target = "limitBy", value = "SIZE_AND_RECORDS") })
    @Documentation("File size unit")
    private SizeUnit sizeUnit;

    @Override
    public String getConfigKey() {
        return dataSet.getConfigKey() + "&debug=" + debug;
    }

    public enum LimitBy {
        SIZE(0x01),
        RECORDS(0x10),
        SIZE_AND_RECORDS(0x11);

        private int flag;

        private LimitBy(int flag) {
            this.flag = flag;
        }

        public boolean isLimitedBySize() {
            return (flag & SIZE.flag) > 0;
        }

        public boolean isLimitedByRecords() {
            return (flag & RECORDS.flag) > 0;
        }
    }

    public enum SizeUnit {
        B(1),
        KB(1 << 10),
        MB(1 << 20),
        GB(1 << 30);

        private long multiplier;

        private SizeUnit(long m) {
            multiplier = m;
        };

        public long apply(long base) {
            return base * multiplier;
        }
    }
}
