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
package org.talend.components.dynamicscrm.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18n {

    String authenticationFailed(String message);

    String connectionFailed(String message);

    String healthCheckOk();

    String entitySetRetrieveFailed(String message);

    String metadataRetrieveFailed(String message);

    String failedParsingBytesValue(String message);

    String failedToDeleteEntity(String entityKey, String message);

    String failedToInsertEntity(String message);

    String idCannotBeNull(String keyFieldName);
}
