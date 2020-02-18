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
package org.talend.components.pubsub.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String successConnection();

    String projectNameRequired();

    String credentialsRequired();

    String errorListTopics(String message);

    String errorListSubscriptions(String message);

    String subscriptionFound(String toString);

    String subscriptionNotFound();

    String subscriptionCreated(String toString);

    String cannotDeleteSubscription(String message);

    String subscriptionDeleted(String subscriptionId);

    String errorCreatePublisher(String message);

    String avroSchemaRequired();

    String avroSchemaInvalid(String message);

    String errorJsonType(String toString);

    String errorReadAVRO(String message);

    String errorReadCSV(String message);

    String errorBadCSV();

    String errorCredentials(String message);

    String errorRemoveSubscription(String message);

    String errorCreateSubscription(String message);

    String errorCreateSubscriber(String message);

    String warnReadMessage(String messageId, String errMessage);

    String inputReleaseWithMessageInbox(int size);
}
