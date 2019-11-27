#! /bin/bash

pre_release_version=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
release_version=$(echo ${pre_release_version}|cut -d- -f1)

# check for snapshot
if [[ $pre_release_version != *'-SNAPSHOT' ]]; then
    echo Cannot release from a non SNAPSHOT, exiting.
    exit
fi

# prepare release
mvn -B -s .jenkins/settings.xml release:clean release:prepare

# perform release
mvn -B -s .jenkins/settings.xml release:perform  -Darguments='-Dmaven.javadoc.skip=true'

post_release_version=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)

# push tag to origin
git push origin release/${release_version}

# We want smooth transition on version with no transition to release
# ie: 1.3.0-SNAPSHOT > 1.3.1-SNAPSHOT
# and NOT 1.3.0-SNAPSHOT > 1.3.0 > 1.3.1-SNAPSHOT

# Reset the current branch to the commit just before the release:
git reset --hard HEAD~2

# Squash merge to next-iter state:
git merge --squash HEAD@{1}

# Commit:
git commit -m "[jenkins-release] prepare for next development iteration $post_release_version"

# push release bump to origin
if [[ ${BRANCH_NAME} == 'master' ]]; then

    # master is out of date, creating a maintenance branch
    master_version=$(echo ${pre_release_version}|sed -e 's/..-SNAPSHOT//')
    git checkout -b         maintenance/$master_version
    git push --force origin maintenance/$master_version

    # bump master
    major=$(     echo ${master_version}|cut -d. -f1)
    minor=$(( $(echo ${master_version}|cut -d. -f2) +1 ))
    next_master_version=${major}.${minor}.0-SNAPSHOT

    # apply bump
    mvn -B -s .jenkins/settings.xml versions:set -DnewVersion=${next_master_version}
    git add -u
    git commit -m "[jenkins-release] prepare for next development iteration $next_master_version"

    # master is a protected branch, should pass by a PR
    git checkout -b    jenkins/master-next-iteration-$next_master_version
    git push -u origin jenkins/master-next-iteration-$next_master_version
else
    # pushed to related origin maintenance branch
    git push --force origin HEAD:${BRANCH_NAME}
fi
