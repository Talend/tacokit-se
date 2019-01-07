package org.talend.components.docker;

import lombok.NoArgsConstructor;
import org.talend.sdk.component.dependencies.maven.Artifact;
import org.talend.sdk.component.dependencies.maven.MvnCoordinateToFileConverter;
import org.tomitribe.crest.Main;
import org.tomitribe.crest.api.Command;
import org.tomitribe.crest.api.Default;
import org.tomitribe.crest.api.Err;
import org.tomitribe.crest.api.Option;
import org.tomitribe.crest.api.Out;
import org.tomitribe.crest.cli.impl.CommandParser;
import org.tomitribe.crest.contexts.DefaultsContext;
import org.tomitribe.crest.contexts.SystemPropertiesDefaultsContext;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

import static java.util.Locale.ROOT;
import static java.util.Optional.ofNullable;
import static lombok.AccessLevel.PRIVATE;

// allows to use CONNECTORS_SE_SETUP_OPTS environment variable to setup a docker image
// ex1: CONNECTORS_SE_SETUP_OPTS="setup --artifact=group1:artifact1:version1 --artifact=group2:artifact2:version2"
// ex2: CONNECTORS_SE_SETUP_OPTS="setup --component-jdbc-auto-download-drivers"
@NoArgsConstructor(access = PRIVATE)
public class ServerSetup {

    @Command
    public static void setup(@Out final PrintStream stdout, @Err final PrintStream stderr,
            @Option("maven-repository-location") @Default("${env.MAVEN_LOCAL_REPOSITORY:-/opt/talend/connectors-se}") final String m2,
            @Option("nexus-base") @Default("http://repo.apache.maven.org/maven2/") final String nexusBase,
            @Option("nexus-token") final String nexusToken,
            @Option("component-jdbc-auto-download-drivers") final boolean downloadDrivers,
            @Option("artifact") final List<String> artifacts) {
        stdout.println("Setting up component server environment");
        try {
            if (artifacts != null) {
                artifacts.forEach(gav -> doInstallFromGav(stdout, stderr, m2, nexusBase, nexusToken, gav));
            }
            if (downloadDrivers) {
                try (final InputStream jdbc = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("local-configuration.properties")) {
                    final Properties properties = new Properties();
                    properties.load(jdbc);
                    properties.stringPropertyNames().stream()
                            .filter(key -> key.startsWith("jdbc.drivers") && key.contains("paths[")).map(properties::getProperty)
                            .forEach(gav -> doInstallFromGav(stdout, stderr, m2, nexusBase, nexusToken, gav));
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            stdout.println("Set up component server environment successfully");
        } catch (final RuntimeException re) {
            stderr.println("An error occured setting up component server environment");
            re.printStackTrace(stderr);
        }
    }

    private static void doInstallFromGav(final PrintStream stdout, final PrintStream stderr, final String m2,
            final String nexusBase, final String nexusToken, final String gav) {
        try {
            if (installFromGav(nexusBase, nexusToken, m2, gav)) {
                stdout.println("Downloaded " + gav + " on " + nexusBase);
            }
        } catch (final RuntimeException re) {
            stderr.println("Failed to download " + gav);
            re.printStackTrace(stderr);
        }
    }

    // only works over http and for releases
    private static boolean installFromGav(final String base, final String token, final String m2, final String gav) {
        final Artifact artifact = new MvnCoordinateToFileConverter().toArtifact(gav);
        final File m2Repo = new File(m2);
        if (!m2Repo.exists() && !m2Repo.mkdirs()) {
            throw new IllegalStateException("Can't create '" + m2 + "'");
        }
        final String path = artifact.toPath();
        final File output = new File(m2Repo, path);
        if (output.exists()) {
            return false;
        }
        if (!output.getParentFile().exists() && !output.getParentFile().mkdirs()) {
            throw new IllegalArgumentException("Can't create " + output);
        }
        try {
            final URL url = new URL(base + (base.endsWith("/") ? "" : "/") + path);
            final HttpURLConnection connection = HttpURLConnection.class.cast(url.openConnection());
            if (token != null) {
                connection.setRequestProperty("Authorization", token);
            }
            try (final InputStream stream = new BufferedInputStream(connection.getInputStream())) {
                Files.copy(stream, output.toPath());
            }
            return true;
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void main(final String[] mainArgs) throws Exception {
        final String[] args = ofNullable(mainArgs).filter(it -> it.length > 0)
                .orElseGet(() -> ofNullable(System.getenv("CONNECTORS_SE_SETUP_OPTS"))
                        .map(value -> new CommandParser().toArgs(value)[0].getArgs()).orElse(mainArgs));

        // if we use the env we still rely on the env else std mode == system props
        final DefaultsContext context = mainArgs == args ? new SystemPropertiesDefaultsContext()
                : (target, commandMethod, key) -> System.getenv(key.replace("-", "_").replace(".", "_").toUpperCase(ROOT));
        final Main main = new Main(context, ServerSetup.class);
        ofNullable(main.exec(args)).ifPresent(System.out::println);
    }
}
