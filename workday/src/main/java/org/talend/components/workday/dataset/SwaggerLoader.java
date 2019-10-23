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
package org.talend.components.workday.dataset;

import io.swagger.models.Operation;
import io.swagger.models.Swagger;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.parser.SwaggerParser;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SwaggerLoader {

    public Map<String, List<WorkdayDataSet.Parameter>> findGetServices() {
        Map<String, List<WorkdayDataSet.Parameter>> resultMap = new TreeMap<>();

        this.files().map(this::fromFiles).forEach(x -> this.addAll(resultMap, x));

        return resultMap;
    }

    /**
     * integrate element of addMap to result.
     * 
     * @param result
     * @param addMap
     */
    private void addAll(Map<String, List<WorkdayDataSet.Parameter>> result, Map<String, List<WorkdayDataSet.Parameter>> addMap) {

        addMap.entrySet().forEach(x -> result.merge(x.getKey(), x.getValue(), (l1, l2) -> {
            l1.addAll(l2);
            return l1;
        }));

    }

    private Map<String, List<WorkdayDataSet.Parameter>> fromFiles(Path swaggerFile) {
        final Swagger sw = new SwaggerParser().read(swaggerFile.toFile().getPath());
        if (sw == null) {
            return Collections.emptyMap();
        }

        return sw.getPaths().entrySet().stream().filter(e -> e.getValue().getGet() != null)
                .collect(Collectors.toMap(e -> sw.getBasePath() + e.getKey(), e -> this.extractParameter(sw, e.getValue())));
    }

    private List<WorkdayDataSet.Parameter> extractParameter(Swagger sw, io.swagger.models.Path servicePath) {
        Operation opGet = servicePath.getGet();
        if (opGet != null) {

            return opGet.getParameters().stream().map(this::mapParameter).collect(Collectors.toList());
        }
        return null;
    }

    private WorkdayDataSet.Parameter mapParameter(Parameter swaggerParam) {
        if (swaggerParam instanceof QueryParameter) {
            return new WorkdayDataSet.Parameter(WorkdayDataSet.Parameter.Type.Query, swaggerParam.getName());
        }
        if (swaggerParam instanceof PathParameter) {
            return new WorkdayDataSet.Parameter(WorkdayDataSet.Parameter.Type.Path, swaggerParam.getName());
        }
        return null;
    }

    private Stream<Path> files() {
        final URL swaggersDirectory = Thread.currentThread().getContextClassLoader().getResource("swaggers");

        try {
            File swaggerRep = new File(swaggersDirectory.getPath());
            return Files.walk(swaggerRep.toPath(), 1).filter((Path p) -> p.toFile().getPath().endsWith(".json"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

}
