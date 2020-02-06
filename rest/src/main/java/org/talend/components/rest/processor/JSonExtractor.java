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
package org.talend.components.rest.processor;

import lombok.RequiredArgsConstructor;
import org.talend.sdk.component.api.processor.OutputEmitter;

import javax.json.JsonObject;
import javax.json.JsonPointer;
import javax.json.JsonValue;
import java.util.Optional;

/**
 * Processor
 */
/*
 * @Version
 * 
 * @Processor
 * 
 * @Icon(Icon.IconType.CUSTOM, "talend-jsonExtractor")
 */
@RequiredArgsConstructor
public class JSonExtractor {

    private final JSonExtractorConfiguration configuration;

    private final JsonExtractorService jsonExtractorService;

    // @ElementListener
    public JsonValue onElement(final JsonObject input) {
        String p = Optional.ofNullable(configuration.getPointer()).orElse("");

        JsonPointer pointer = jsonExtractorService.getJsonProvider().createPointer(p);
        return pointer.getValue(input);
    }
}
