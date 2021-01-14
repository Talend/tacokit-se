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
package org.talend.components.marketo.output;

import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.service.MarketoService;

public abstract class OutputComponentStrategy extends MarketoSourceOrProcessor implements ProcessorStrategy {

    protected final MarketoOutputConfiguration configuration;

    public OutputComponentStrategy(final MarketoOutputConfiguration configuration, //
            final MarketoService service) {
        super(configuration.getDataSet(), service);
        this.configuration = configuration;
    }

    @Override
    public void init() {
        super.init();
    }
}
