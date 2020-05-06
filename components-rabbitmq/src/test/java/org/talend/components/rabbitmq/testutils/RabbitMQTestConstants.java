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
package org.talend.components.rabbitmq.testutils;

public class RabbitMQTestConstants {

    public static final String HOSTNAME = "localhost";

    public static final Integer PORT = 5671;

    public final static Integer HTTP_PORT = 15672;

    public static final String USER_NAME = "user";

    public static final String PASSWORD = "bitnami";

    public static final String QUEUE_NAME = "test";

    public static final String DIRECT_EXCHANGE_NAME = "directex";

    public static final String FANOUT_EXCHANGE_NAME = "fanoutex";

    public static final String TEST_MESSAGE = "hello world";

    public static final int MAXIMUM_MESSAGES = 1;

}
