#! /bin/bash

echo "Packaging locales"
mvn  ${EXTRA_BUILD_PARAMS} -e -B -s .jenkins/settings.xml clean package -pl . -Pi18n-deploy
if [[ ! $? -eq 0 ]]; then
  echo mvn error during xtm packaging
  exit 123
fi

ls -al ./target
ls -alR ~/.m2/repository/org/talend/components/

echo "Fixing locales to lang only and not lang_COUNTRY"
cd tmp/repository
arr=("fr_FR:fr" "ja_JP:ja" "cn_CZ:cn_CZ" "en_US:en") && for aa in ${arr[@]}; do
  i18n=${aa%%:*} && tck=${aa#*:}
  (find . -name "Messages_${i18n}.properties" | sed -e "s,\(.*\),mv \1 \1," | sed -e "s,${i18n},${tck},2") | /bin/bash
done

git add --ignore-removal .
git commit -m"fix(locales): rename locales to lang only"
git push

echo "Deploying locales"
ls -al
mvn ${EXTRA_BUILD_PARAMS} -X -s ../../.jenkins/settings.xml clean deploy -DaltDeploymentRepository=talend_nexus_deployment::default::https://artifacts-zl.talend.com/nexus/content/repositories/TalendOpenSourceRelease/
if [[ ! $? -eq 0 ]]; then
  echo mvn error during xtm deploying
  exit 123
fi
