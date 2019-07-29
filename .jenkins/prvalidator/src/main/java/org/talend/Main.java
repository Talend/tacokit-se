package org.talend;

import edu.umd.cs.findbugs.BugInstance;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.talend.bugs.BugsFinder;
import org.talend.github.PullRequestExtractor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

public class Main {

    public static void main(String... args) throws IOException {

        int reqNumber = Integer.parseInt(args[2]);
        String commitSha = args[3];

        GitHub github = GitHub.connectUsingPassword(args[0], args[1]);

        GHRepository repo = github.getRepository("Talend/" + args[4]);

        Path rep = Paths.get(args[5]);

        PullRequestExtractor ext = new PullRequestExtractor(repo);

        BugsFinder finder = new BugsFinder();
        final Collection<BugInstance> bugs = finder.findAll(rep);

        final List<String> sourceFiles = ext.fileListFromPullRequest(reqNumber);

        String comments = new CommentBuilder().buildComment(sourceFiles, bugs);

        System.out.println("Send comments : " + comments);

        ext.addComment(reqNumber, comments);
    }

}
