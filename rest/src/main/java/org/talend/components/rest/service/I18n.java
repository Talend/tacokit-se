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
package org.talend.components.rest.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18n {

    String healthCheckStatus(final String url, final int code);

    String healthCheckException(final String trace);

    String healthCheckOk();

    String healthCheckFailed(final String URL);

    String malformedURL(final String URL, final String cause);

    String badRequestBody(final String type);

    String requestStatus(final int status);

    String redirect(final int nbRedirect, final String url);

    String request(final String method, final String url, final String authentication);

    String bodyContentLength(final int length);

    String setConnectionTimeout(final int timeout);

    String setReadTimeout(final int timeout);

    String addContentTypeHeader(final String name, final String value);

    String timeout(final String url, final String message);

    String duplicateKeys(final String entity);

    String headers();

    String queryParameters();

    String pathParameters();

    String bodyParameters();

    String withoutFollowRedirectsDegradedMode();

    String invalideBodyContent(String format, String cause);

    String formatText();

    String formatJSON();

    String notValidAddress(boolean canAccessLocal, boolean disableMulticast);

}
