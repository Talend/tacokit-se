#! /bin/bash

# git config hack when pushing to bypass :
# "fatal: could not read Username for 'https://github.com': No such device or address" error.
# This appeared after 2fa auth activation on github.
git config --global credential.username ${GITHUB_LOGIN}
git config --global credential.helper '!echo password=${GITHUB_TOKEN}; echo'
git config --global credential.name "jenkins-build"
env | sort

pre_release_version=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
release_version=$(echo ${pre_release_version} | cut -d- -f1)

# check for snapshot
if [[ $pre_release_version != *'-SNAPSHOT' ]]; then
  echo Cannot release from a non SNAPSHOT, exiting.
  exit 123
fi

# prepare release
mvn -B -s .jenkins/settings.xml release:clean release:prepare ${EXTRA_BUILD_PARAMS} -Dtalend.maven.decrypter.m2.location=${WORKSPACE}/.jenkins/
if [[ ! $? -eq 0 ]]; then
  echo mvn error during build
  exit 123
fi

# perform release
mvn -B -s .jenkins/settings.xml release:perform ${EXTRA_BUILD_PARAMS} -Darguments='-Dmaven.javadoc.skip=true' -Dtalend.maven.decrypter.m2.location=${WORKSPACE}/.jenkins/
if [[ ! $? -eq 0 ]]; then
  echo mvn error during build
  exit 123
fi
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
  master_version=$(echo ${pre_release_version} | sed -e 's/..-SNAPSHOT//')
  git checkout -b maintenance/$master_version
  git push --force origin maintenance/$master_version

  # bump master
  major=$(echo ${master_version} | cut -d. -f1)
  minor=$(($(echo ${master_version} | cut -d. -f2) + 1))
  next_master_version=${major}.${minor}.0-SNAPSHOT

  # apply bump
  mvn -B -s .jenkins/settings.xml versions:set -DnewVersion=${next_master_version}
  mvn versions:set-property -Dproperty=locales.version -DnewVersion="[${major}.${minor},)"
  mvn versions:set-property -Dproperty=common.version -DnewVersion=${next_master_version}
  mvn versions:set-property -Dproperty=connectors-test-bom.version -DnewVersion=${next_master_version}
  git add -u
  git commit -m "[jenkins-release] prepare for next development iteration $next_master_version"

  # master is a protected branch, should pass by a PR
  git checkout -b jenkins/master-next-iteration-$next_master_version
  git push -u origin jenkins/master-next-iteration-$next_master_version
else
  # pushed to related origin maintenance branch
  git push --force origin HEAD:${BRANCH_NAME}
fi
