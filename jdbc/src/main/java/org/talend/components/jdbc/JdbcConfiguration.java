// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jdbc;

import java.util.ArrayList;
import java.util.List;

import javax.json.bind.annotation.JsonbProperty;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.configuration.Configuration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * the bean class for the database information
 */
@Data
@Documentation("Jdbc component configuration")
public class JdbcConfiguration {

    @Option
    @Documentation("list of driver meta data")
    private List<Driver> drivers;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    @Documentation("Jdbc driver metadata")
    public static class Driver {

        @Option
        @Documentation("Jdbc driver id")
        private String id;

        @Option
        @Documentation("Jdbc driver class")
        private String className;

        @Option
        @Documentation("Jdbc url ")
        private String url;

        @Option
        @Documentation("Jdbc driver and driver dependencies jar locations")
        private List<Path> paths = new ArrayList<>();

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Path {

            @Option
            @Documentation("Jdbc driver and driver dependencies jar locations in mvn format")
            private String path;
        }
    }

}
