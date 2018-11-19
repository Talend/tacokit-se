/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * the bean class for the database information
 */
@Data
@NoArgsConstructor
@Documentation("Jdbc component configuration")
public class JdbcConfiguration implements Serializable {

    @Option
    @Documentation("connection configuration. like timeouts")
    private Connection connection;

    @Option
    @Documentation("list of driver meta data")
    private final List<Driver> drivers = new ArrayList<>();

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(of = { "id", "className" })
    @Documentation("Jdbc driver metadata")
    public static class Driver implements Serializable {

        @Option
        @Documentation("Jdbc driver id. this is must be unique")
        private String id;

        @Option
        @Documentation("Jdbc driver class")
        private String className;

        @Option
        @Documentation("Jdbc url example")
        private String url;

        @Option
        @Documentation("Jdbc driver and driver dependencies jar locations")
        private final List<Path> paths = new ArrayList<>();

        @Data
        @NoArgsConstructor
        public static class Path implements Serializable {

            @Option
            @Documentation("Jdbc driver and driver dependencies jar locations in mvn format")
            private String path;
        }
    }

    @Data
    @NoArgsConstructor
    public static class Connection implements Serializable {

        @Option
        @Documentation("Connection validation timeout in seconds")
        private Integer validationTimeout;
    }

}
