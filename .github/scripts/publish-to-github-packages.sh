#!/usr/bin/env bash
# Publishes runner, lib and api to GitHub Packages, skipping any module whose version is already there.
#
# GitHub Packages refuses to overwrite a release version, so re-running a release after a later step
# (Maven Central, Docker) failed must not upload the same version again. A 200, or a redirect to the file's
# storage location, means it is there; only a 404 counts as "not there yet"; any other answer stops the
# run, so an auth problem or an outage never turns into an upload attempt.
#
# Usage: VERSION=0.5.0 GITHUB_ACTOR=... GITHUB_TOKEN=... .github/scripts/publish-to-github-packages.sh
set -euo pipefail

version="${VERSION:?VERSION is required}"
actor="${GITHUB_ACTOR:?GITHUB_ACTOR is required}"
token="${GITHUB_TOKEN:?GITHUB_TOKEN is required}"
registry="https://maven.pkg.github.com/fleaktech/zephflow-core"

# nebula.release takes the highest tag on HEAD, so a commit carrying another tag can build a
# different version than the one checked below. Refuse rather than check one version and upload another.
built_version=$(./gradlew -q -Prelease.useLastTag=true properties | sed -n 's/^version: //p')
if [ "$built_version" != "$version" ]; then
  echo "::error::Gradle builds version '$built_version' from this commit, but the tag asks for $version" >&2
  exit 1
fi

tasks=()
for module in runner lib api; do
  pom_url="$registry/io/fleak/zephflow/$module/$version/$module-$version.pom"
  # Credentials go through a netrc file descriptor so they never appear in the process list.
  status=$(curl -sS -o /dev/null -w '%{http_code}' \
    --netrc-file <(printf 'machine maven.pkg.github.com login %s password %s\n' "$actor" "$token") \
    "$pom_url")
  case "$status" in
    200 | 301 | 302 | 303 | 307 | 308)
      # A version already there was built by an earlier run, possibly from another commit; say so on the run page.
      echo "::warning::$module $version is already on GitHub Packages; skipping it, so it is not rebuilt from this commit."
      ;;
    404) tasks+=("$module:publishAllPublicationsToGitHubPackagesRepository") ;;
    *)
      echo "::error::Checking $module $version on GitHub Packages returned HTTP $status" >&2
      exit 1
      ;;
  esac
done

if [ "${#tasks[@]}" -eq 0 ]; then
  echo "runner, lib and api $version are all on GitHub Packages already."
  exit 0
fi

./gradlew clean
./gradlew -Prelease.useLastTag=true "${tasks[@]}" --stacktrace
