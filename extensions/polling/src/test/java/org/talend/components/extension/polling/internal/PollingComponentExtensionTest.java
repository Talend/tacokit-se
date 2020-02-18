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
package org.talend.components.extension.polling.internal;

import org.junit.jupiter.api.Test;
import org.talend.components.extension.polling.internal.impl.PollingConfiguration;
import org.talend.components.extension.polling.mapper.Config;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents(value = "org.talend.components.extension.polling.mapper")
class PollingComponentExtensionTest {

    @Injected
    private BaseComponentsHandler handler;

    @Test
    void testPollingComponentExtension() {

        final Config mainConfig = new Config();
        mainConfig.setParam0(1234);
        mainConfig.setParam1("abcde");

        PollingConfiguration pollingConf = new PollingConfiguration();
        pollingConf.setDelay(1000);

        final String mainConfigStr = configurationByExample().forInstance(mainConfig)
                .withPrefix("configuration.BatchSource.configuration").configured().toQueryString();

        final String pollingConfigStr = configurationByExample().forInstance(pollingConf).withPrefix(
                "configuration." + PollingComponentExtension.POLLING_CONFIGURATION_KEY + ".internal_polling_configuration")
                .configured().toQueryString();

        Job.components() //
                .component("emitter", "test://MyPollable?" + String.join("&", mainConfigStr, pollingConfigStr)) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);

    }

}