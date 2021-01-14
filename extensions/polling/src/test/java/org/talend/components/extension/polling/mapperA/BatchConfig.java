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
package org.talend.components.extension.polling.mapperA;

import lombok.Data;
import org.talend.components.extension.polling.api.PollableDuplicateDataset;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row({ "dse" }), @GridLayout.Row({ "param1" }), @GridLayout.Row({ "param0" }) })
public class BatchConfig implements Serializable {

    @Option
    BatchDataset dse;

    @Option
    String param1;

    @Option
    int param0;

    @Version(1)
    @Data
    @DataSet("batchDatasetName")
    @Documentation("batchDataset documentation")
    @GridLayout({ @GridLayout.Row({ "dso" }), @GridLayout.Row({ "paramdse" }) })
    @PollableDuplicateDataset
    public class BatchDataset {

        @Option
        @Documentation("The datastore")
        BatchDatastore dso;

        @Option
        @Documentation("Param of dataset")
        int paramdse;

    }

    @Version(1)
    @Data
    @DataStore("batchDatastoreName")
    @Documentation("batchDatastore documentation")
    @GridLayout({ @GridLayout.Row({ "paramdso" }) })
    public class BatchDatastore {

        @Option
        @Documentation("Param of datastore")
        String paramdso;

    }

}
