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
package org.talend.components.adlsgen2.runtime;

import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;

import lombok.Getter;

public class AdlsDatasetRuntimeInfo extends AdlsDatastoreRuntimeInfo {

    @Getter
    private AdlsGen2DataSet dataSet;

    public AdlsDatasetRuntimeInfo(AdlsGen2DataSet dataSet, AdlsActiveDirectoryService tokenProviderService) {
        super(dataSet.getConnection(), tokenProviderService);
        prepareRuntimeInfo(dataSet);
    }

    protected void prepareRuntimeInfo(AdlsGen2DataSet dataSet) {
        super.prepareRuntimeInfo(dataSet.getConnection());
        this.dataSet = dataSet;
    }

    @Override
    public AdlsGen2Connection getConnection() {
        return dataSet.getConnection();
    }
}
