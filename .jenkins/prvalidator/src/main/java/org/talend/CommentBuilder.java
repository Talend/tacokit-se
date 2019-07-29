package org.talend;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.SourceLineAnnotation;

import java.util.Collection;
import java.util.List;

public class CommentBuilder {

    public String buildComment(List<String> pullRequestFiles, Collection<BugInstance> bugs) {
        StringBuilder commentBuilder = new StringBuilder();
        for (final String requestFile : pullRequestFiles) {
            String fileBugComment = bugs.stream()
                    .filter((BugInstance b) -> this.sameSource(requestFile, b))
                    .map(this::bugComment)
                    .reduce("", (String s1, String s2) -> s1 + System.lineSeparator() + s2);

            if (fileBugComment != null && !fileBugComment.isEmpty()) {
                commentBuilder.append(requestFile);
                commentBuilder.append(System.lineSeparator());
                commentBuilder.append(fileBugComment);
                commentBuilder.append(System.lineSeparator());
                commentBuilder.append(System.lineSeparator());
            }

        }
        return commentBuilder.toString();
    }

    private String bugComment(BugInstance bug) {
        final SourceLineAnnotation lineAnnotation = bug.getPrimarySourceLineAnnotation();
        int start = lineAnnotation.getStartLine();
        int end = lineAnnotation.getEndLine();
        StringBuilder commentBuilder = new StringBuilder("lines ");
        commentBuilder.append(start);
        if (end > start) {
            commentBuilder.append("-").append(end);
        }
        commentBuilder.append(System.lineSeparator())
                .append(bug.getMessageWithPriorityTypeAbbreviation());

        return commentBuilder.toString();
    }


    private boolean sameSource(String pullRequestFile, BugInstance bug) {
        return pullRequestFile.endsWith(bug.getPrimarySourceLineAnnotation().getSourcePath());
    }
}
