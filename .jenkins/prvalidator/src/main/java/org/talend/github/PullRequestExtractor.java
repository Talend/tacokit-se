package org.talend.github;

import org.kohsuke.github.*;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PullRequestExtractor {

    private final GHRepository repository;

    public PullRequestExtractor(GHRepository repository) {
        this.repository = repository;
    }


    /**
     * list of java modified file of a pull request.
     *
     * @param requestNumber : request number.
     * @return list modified file.
     * @throws IOException exception if pull request doesn't exist.
     */
    public List<String> fileListFromPullRequest(int requestNumber) throws IOException {

        GHPullRequest request = this.repository.getPullRequest(requestNumber);
        PagedIterable<GHPullRequestFileDetail> files = request.listFiles();

        return files.asList().stream() // flux modified file
                .map(GHPullRequestFileDetail::getFilename) // keep file name
                .filter(Objects::nonNull)
                .filter((String filename) -> filename.endsWith(".java")) // only java.
                .collect(Collectors.toList());
    }

    /**
     * add a commment in merge request.
     *
     * @param requestNumber : request number.
     * @param commment
     * @throws IOException
     */
    public void addComment(int requestNumber, String commment) {
        try {
            GHPullRequest request = this.repository.getPullRequest(requestNumber);
            final List<GHPullRequestReview> reviews = request.listReviews().asList();
            boolean alreadyComment = reviews.stream()
                    .map(GHPullRequestReview::getBody)
                    .anyMatch((String body) -> Objects.equals(commment, body));

            if (!alreadyComment) {
                System.out.println("reviews empty : create");
                final GHPullRequestReview review = request.createReview().body("Code review").create();
                review.submit(commment, GHPullRequestReviewEvent.COMMENT);
            }
        }
        catch (IOException | RuntimeException ex) {
            System.out.println("Non blocking exception on PR comments :" + ex.getMessage());
            ex.printStackTrace(System.out);
        }
    }
}
