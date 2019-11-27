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
package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataSet
@AllArgsConstructor
@NoArgsConstructor
@GridLayout({ @GridLayout.Row({ "magentoDataStore" }), @GridLayout.Row({ "selectionType" }), })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoDataStore" }),
        @GridLayout.Row({ "selectionType" }) })
@Documentation("Data set configuration")
public class MagentoDataSet implements Serializable {

    @Option
    @Documentation("Connection to Magento CMS")
    private MagentoDataStore magentoDataStore;

    @Option
    @Documentation("The type of information we want to get/save, e.g. 'Products'")
    private SelectionType selectionType = SelectionType.PRODUCTS;

    public String getMagentoUrl() {
        String res = magentoDataStore.getMagentoBaseUrl() + "/" + selectionType.name().toLowerCase();
        return res;
    }
}