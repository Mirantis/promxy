#!/usr/bin/env bash
# Opens one issue when the latest jacksontj/promxy release is not yet merged
# into the mirantis branch. Run by .github/workflows/upstream-watch.yml.
#
# Inputs (environment):
#   UPSTREAM_REPO      upstream repository, e.g. jacksontj/promxy
#   BASE_REF           git ref of the downstream branch, e.g. origin/mirantis
#   GITHUB_REPOSITORY  repository the issue is opened in, e.g. Mirantis/promxy
#   LABEL              label put on the issue
#   GH_TOKEN           token for gh (issues: write)
#   DRY_RUN=1          print the issue instead of creating it
set -euo pipefail

TAG="$(gh api "repos/${UPSTREAM_REPO}/releases/latest" --jq .tag_name)"
git fetch -q --no-tags "https://github.com/${UPSTREAM_REPO}.git" "refs/tags/${TAG}"
RELEASE="$(git rev-parse 'FETCH_HEAD^{commit}')"

if git merge-base --is-ancestor "${RELEASE}" "${BASE_REF}"; then
  echo "${BASE_REF} already contains upstream ${TAG} (${RELEASE:0:8}); nothing to do."
  exit 0
fi

TITLE="Upstream release ${TAG}"

# One issue per release, ever: an open issue is still being handled, and a
# closed one means someone decided to skip this release.
EXISTING="$(gh issue list --repo "${GITHUB_REPOSITORY}" --state all --label "${LABEL}" \
  --limit 200 --json number,title --jq "map(select(.title == \"${TITLE}\")) | .[0].number // empty")"
if [ -n "${EXISTING}" ]; then
  echo "Issue #${EXISTING} already tracks ${TAG}; nothing to do."
  exit 0
fi

MISSING="$(git rev-list --count "${BASE_REF}..${RELEASE}")"

# Merge without touching the work tree to predict conflicts. Output is the
# tree id, then one conflicted path per line, then a blank line.
RC=0
CONFLICTS="$(git merge-tree --write-tree --name-only "${BASE_REF}" "${RELEASE}")" || RC=$?
# shellcheck disable=SC2016 # the backticks are literal Markdown code spans
case "${RC}" in
  0) CONFLICT_TEXT="None predicted: the merge applies cleanly." ;;
  1) CONFLICT_TEXT="$(printf '%s\n' "${CONFLICTS}" | sed -n '2,/^$/p' | sed '/^$/d' | sed 's/^/- `/; s/$/`/')" ;;
  *) CONFLICT_TEXT="Could not predict (git merge-tree exited with ${RC})." ;;
esac

BODY="$(cat <<EOF
Upstream published [${TAG}](https://github.com/${UPSTREAM_REPO}/releases/tag/${TAG}), and \`${BASE_REF#origin/}\` does not contain it yet: ${MISSING} upstream commit(s) are missing.

**Predicted conflicts:**
${CONFLICT_TEXT}

**To merge it** (see the README, "Mirantis downstream"):

\`\`\`bash
git fetch upstream --tags
git switch -c sync/upstream-${TAG} origin/mirantis
git merge --no-ff ${TAG}
# resolve conflicts; for go.mod/vendor run: go mod tidy && go mod vendor
git push origin sync/upstream-${TAG}
\`\`\`

Open the PR against \`mirantis\` and merge it with **Create a merge commit**. Close this issue once it is merged, or close it with a comment if we skip this release.
EOF
)"

if [ "${DRY_RUN:-0}" = "1" ]; then
  printf 'DRY RUN, would open issue in %s\nTitle: %s\n\n%s\n' "${GITHUB_REPOSITORY}" "${TITLE}" "${BODY}"
  exit 0
fi

gh label create "${LABEL}" --repo "${GITHUB_REPOSITORY}" --color 0E8A16 \
  --description "A new upstream release to merge into mirantis" 2>/dev/null || true
gh issue create --repo "${GITHUB_REPOSITORY}" --title "${TITLE}" --label "${LABEL}" --body "${BODY}"
