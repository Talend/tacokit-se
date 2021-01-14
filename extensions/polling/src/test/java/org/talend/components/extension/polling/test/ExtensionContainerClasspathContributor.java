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
package org.talend.components.extension.polling.test;

import org.talend.sdk.component.dependencies.maven.Artifact;
import org.talend.sdk.component.runtime.manager.ComponentManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ExtensionContainerClasspathContributor implements ComponentManager.ContainerClasspathContributor {

    private final Artifact artifact = new Artifact("talend", "extension", "jar", null, "1", "compile");;

    @Override
    public Collection<Artifact> findContributions(final String pluginId) {
        return "test-classes".equals(pluginId) ? singletonList(artifact) : emptyList();
    }

    @Override
    public boolean canResolve(final String path) {
        return path.equals(artifact.toPath());
    }

    @Override
    public Path resolve(final String path) {
        return Paths.get("target/classes");
    }
}
