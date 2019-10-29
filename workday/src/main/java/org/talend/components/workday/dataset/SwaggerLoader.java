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
import lombok.Data;
import org.talend.sdk.component.api.service.completion.Values;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Load swagger files from a folder.
 * TODO : see https://jira.talendforge.org/browse/TDI-42102 to add specific conf
 */
public class SwaggerLoader {

    /** all availables swaggers */
    private final Map<String, Swagger> swaggers = new HashMap<>();

    /**
     * Constructor
     * 
     * @param swaggersPath : path for swaggers (cab be inside jar file).
     */
    public SwaggerLoader(String swaggersPath) {
        this.init(swaggersPath);
    }

    /**
     * All module name (that contains get services).
     * 
     * @return
     */
    public Collection<Values.Item> getModules() {
        return this.swaggers.entrySet().stream().filter((Map.Entry<String, Swagger> e) -> this.isSwaggerOK(e.getValue()))
                .map((Map.Entry<String, Swagger> e) -> new Values.Item(e.getKey(), e.getValue().getInfo().getTitle()))
                .collect(Collectors.toList());
    }

    public Map<String, List<WorkdayDataSet.Parameter>> findGetServices(String id) {
        final Swagger sw = swaggers.get(id);
        return this.fromSwagger(sw);
    }

    private boolean isSwaggerOK(Swagger sw) {
        return sw != null && sw.getInfo() != null && sw.getInfo().getTitle() != null
                && sw.getPaths().values().stream().anyMatch((io.swagger.models.Path p) -> p.getGet() != null);
    }

    private Map<String, List<WorkdayDataSet.Parameter>> fromSwagger(Swagger sw) {
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

    private void init(String swaggersPath) {
        try {
            SwaggerParser parser = new SwaggerParser();

            this.findSwaggers(swaggersPath).map((TUple<String, InputStream> e) -> e.secondMap(this::inputStreamToString))
                    .map((TUple<String, String> sw) -> sw.secondMap(parser::parse))
                    .forEach((TUple<String, Swagger> sw) -> this.swaggers.put(sw.getU(), sw.getV()));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to load workday swaggers files", e);
        }
    }

    private String inputStreamToString(InputStream content) {
        try (Scanner scanner = new Scanner(content, StandardCharsets.UTF_8.name())) {
            return scanner.useDelimiter("\\A").next();
        }
    }

    private Stream<TUple<String, InputStream>> findSwaggers(String path) throws IOException {
        if (path.contains(".jar!/")) {
            return this.entries(path);
        } else {
            File swaggerRep = new File(path);
            return Files.walk(swaggerRep.toPath(), 1).filter((Path p) -> p.toFile().getPath().endsWith(".json"))
                    .map((Path p) -> TUple.of(p.toFile().getName(), getInputStream(p)));

        }
    }

    private InputStream getInputStream(Path p) {
        try {
            return Files.newInputStream(p);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't read file " + p.toFile().getPath(), e);
        }
    }

    private Stream<TUple<String, InputStream>> entries(String pathJar) throws IOException {
        String realpath = pathJar;
        int pos = pathJar.indexOf(".jar!/");
        if (pos >= 0) {
            realpath = realpath.substring(0, pos + 4);
            if (realpath.startsWith("file:")) {
                realpath = realpath.substring(5);
            }
        }
        String repo = pathJar.substring(pos + 6);
        JarFile jar = new JarFile(realpath);
        Enumeration<JarEntry> entries = jar.entries();
        return Commons.enumerationAsStream(entries)
                .filter((JarEntry e) -> e.getName().startsWith(repo) && e.getName().endsWith(".json"))
                .map((JarEntry e) -> TUple.of(e.getName(), this.getInputStream(jar, e)));
    }

    private InputStream getInputStream(JarFile jar, JarEntry entry) {
        try {
            return jar.getInputStream(entry);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't extract " + entry.getName() + " from " + jar.getName(), e);
        }
    }

    @Data
    private static class TUple<U, V> {

        private final U u;

        private final V v;

        public <U1> TUple<U1, V> firstMap(Function<U, U1> f) {
            return new TUple<>(f.apply(u), v);
        }

        public <V1> TUple<U, V1> secondMap(Function<V, V1> f) {
            return new TUple<>(u, f.apply(v));
        }

        public static <A, B> TUple<A, B> of(A a, B b) {
            return new TUple<A, B>(a, b);
        }

    }
}
