package org.talend.components.jdbc;

import org.jboss.shrinkwrap.resolver.api.maven.ScopeType;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependencies;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependency;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;

// TODO remove this when available in tacokit
public class BeamDirectRunner extends DirectRunnerEnvironment {

    @Override
    protected MavenDependency[] rootDependencies() {
        return new MavenDependency[] {
                MavenDependencies.createDependency(rootDependencyBase() + ":jar:2.8.0", ScopeType.RUNTIME, false),
                MavenDependencies.createDependency("org.talend.sdk.component:component-runtime-beam:jar:1.1.3-SNAPSHOT",
                        ScopeType.RUNTIME, false) };
    }
}
