#!/bin/sh
set -eu
cd "$(dirname "$0")"
project_dir=$(pwd)

# JitPack's default Maven cannot build this SDK revision. Build its public source
# with our Maven, retaining the commit as the installed dependency's version.
revision=$(mvn -q org.apache.maven.plugins:maven-help-plugin:3.5.1:evaluate -Dexpression=globex.sdk.version -DforceStdout)
case "$revision" in ''|*[!0-9a-f]*) echo 'Invalid SDK revision' >&2; exit 1 ;; esac
test "${#revision}" -eq 40
sdk_dir="target/sdk-$revision"
mkdir -p "$sdk_dir"
curl --fail --location --retry 3 --silent --show-error \
  "https://api.github.com/repos/openmdta/sdk-globex-java/tarball/$revision" \
  -o target/sdk.tar.gz
tar -xzf target/sdk.tar.gz --strip-components=1 -C "$sdk_dir"
(
  cd "$sdk_dir"
  mvn -s "$project_dir/.mvn/settings.xml" -B -ntp org.codehaus.mojo:versions-maven-plugin:2.19.1:set \
    -DnewVersion="$revision" -DgenerateBackupPoms=false
  mvn -s "$project_dir/.mvn/settings.xml" -B -ntp install
)
mvn -B -ntp verify
