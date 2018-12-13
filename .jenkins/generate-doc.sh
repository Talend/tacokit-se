#! /bin/bash

OUTPUT_DIR=target/talend-component-kit_documentation/
INDEX_OUTPUT=$OUTPUT_DIR/index.html

rm -Rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

modules=$(grep '^    <module>' pom.xml | sed 's/.*>\(.*\)<.*/\1/' | sort -u | grep -v common)
mvn -Pdocumentation-html dependency:unpack@doc-html-theme
mvn -Pdocumentation-html -T1C asciidoctor:process-asciidoc@html \
    $(echo $modules | sed 's/[^ ]* */-pl &/g')

echo "<html><head><title>Documentations</title></head><body>" > $INDEX_OUTPUT
echo "<h1>Reports</h1>" >> $INDEX_OUTPUT
echo "<ul>" >> $INDEX_OUTPUT
echo "$(echo $modules | sed 's/[^ ]*/<li><a href="&\/documentation.html">&<\/a><\/li>/g')" >> $INDEX_OUTPUT
echo "</ul>" >> $INDEX_OUTPUT
echo "</body></html>" >> $INDEX_OUTPUT
