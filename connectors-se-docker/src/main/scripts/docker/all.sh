#! /bin/bash

#
#  Copyright (C) 2006-2018 Talend Inc. - www.talend.com
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

scriptBase="$(dirname $0)"

. "$scriptBase/common.sh" $1
export DOCKER_HTML="$BASEDIR/target/docker.html"

mkdir -p $(dirname "$DOCKER_HTML")
echo "<!DOCTYPE html>" > "$DOCKER_HTML"
echo "<html>" >> "$DOCKER_HTML"
echo "  <head>" >> "$DOCKER_HTML"
echo "    <style>" >> "$DOCKER_HTML"
echo "    table, th, td { border: 1px solid black; border-collapse: collapse; }" >> "$DOCKER_HTML"
echo "    </style>" >> "$DOCKER_HTML"
echo "  </head>" >> "$DOCKER_HTML"
echo "  </body>" >> "$DOCKER_HTML"
echo "    <h2>Docker Images</h2>" >> "$DOCKER_HTML"
echo "    <table>" >> "$DOCKER_HTML"
echo "      <tr>" >> "$DOCKER_HTML"
echo "        <th>Repository</th>" >> "$DOCKER_HTML"
echo "        <th>Image</th>" >> "$DOCKER_HTML"
echo "        <th>Version</th>" >> "$DOCKER_HTML"
echo "        <th>Tag</th>" >> "$DOCKER_HTML"
echo "      </tr>" >> "$DOCKER_HTML"

cat $DOCKER_HTML

. "$scriptBase/repository.sh"
. "$scriptBase/server.sh"

echo "    </table>" >> "$DOCKER_HTML"
echo "  </body>" >> "$DOCKER_HTML"
echo "</html>" >> "$DOCKER_HTML"
