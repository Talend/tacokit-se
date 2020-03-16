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
package org.talend.components.netsuite.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface Messages {

    String healthCheckOk();

    String healthCheckFailed(final String cause);

    String failedToLogin(final String cause);

    String bindingError();

    String searchRecordNotFound(String recordTypeName);

    String advancedSearchNotAllowed(String type);

    String searchNotAllowed(String recordTypeName);

    String invalidDataType(String type);

    String cannotGetDataTypeFactory();

    String bindingTypeError(String typeNameToRegister, String name, String name2);

    String invalidSearchType(String fieldTypeName);

    String recordTypeNotFound(String typeName);

    String unknownSearchFieldOperator(String fieldTypeName, String operatorName);

    String cannotCreateInstance(String typeName);

    String cannotRetrieveCustomizations();

    String cannotRetrieveCustomizationIds(String name);

    String cannotLogoutFromNetSuite();

    String ssoLoginNotSupported();

    String couldNotGetWebServiceDomain(String defaultEndpointUrl);

    String failedToInitClient(String message);

    String endpointUrlRequired();

    String accountRequired();

    String emailRequired();

    String passwordRequired();

    String roleRequired();

    String consumerKeyRequired();

    String consumerSecretRequired();

    String tokenIdRequired();

    String tokenSecretRequired();

    String endpointUrlApiVersionMismatch(String endpoint, String apiVersion);
}