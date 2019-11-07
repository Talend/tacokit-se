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
package org.talend.components.workday.input;

import org.talend.components.workday.dataset.WorkdayServiceDataSet;
import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

@Version(1)
@Icon(value = Icon.IconType.BURGER) // FIXME : find a real icon.
@Emitter(family = "Workday", name = "Input")
@Documentation("Component to extract data from workday ERP via REST services")
public class WorkdayServiceProducer implements Serializable {

    private static final long serialVersionUID = -293252782589800593L;

    private static final int limit = 100;

    private final InputConfiguration inputConfig;

    private transient final WorkdayReaderService reader;

    private transient int total = -1;

    private transient Supplier<JsonObject> supplierObject;

    public WorkdayServiceProducer(@Option("configuration") InputConfiguration inputConfig, WorkdayReaderService reader) {
        this.inputConfig = inputConfig;
        this.reader = reader;
    }

    @PostConstruct
    public void init() {
        BufferedProducerIterator<JsonObject> producerIterator = new BufferedProducerIterator<>(this::elementsOfPage);
        this.supplierObject = producerIterator::next;
    }

    @Producer
    public JsonObject next() {
        return supplierObject.get();
    }

    private Iterator<JsonObject> elementsOfPage(int pageNumber) {
        JsonObject jsonRet = this.getPageContent(pageNumber);
        if (jsonRet == null) {
            return null;
        }
        JsonArray data = jsonRet.getJsonArray("data");
        return data.stream().map(JsonObject.class::cast).iterator();
    }

    /**
     * Extract page content.
     * 
     * @param pageNumber : page number to extract.
     * @return extracted page.
     */
    private JsonObject getPageContent(int pageNumber) {

        if (this.total >= 0 && (pageNumber * WorkdayServiceProducer.limit) >= this.total) {
            return null;
        }
        final WorkdayServiceDataSet ds = this.inputConfig.getDataSet();
        Map<String, String> queryParams = ds.extractQueryParam();

        JsonObject ret = null;
        if (ds.getParameters() != null && ds.getParameters().isPaginable()) {
            ret = this.reader.findPage(ds.getDatastore(), ds, (pageNumber * WorkdayServiceProducer.limit),
                    WorkdayServiceProducer.limit, queryParams);
            if (this.total < 0) {
                synchronized (this) {
                    this.total = ret.getInt("total");
                }
            }
        } else {
            ret = this.reader.find(ds.getDatastore(), ds, queryParams);
            this.total = 0;
        }
        return ret;
    }

}
