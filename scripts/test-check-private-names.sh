#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
check_script="${repository_root}/scripts/check-private-names.sh"
pre_push_hook="${repository_root}/.githooks/pre-push"
workspace="$(mktemp -d)"
trap 'rm -rf "${workspace}"' EXIT
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=Tester GIT_AUTHOR_EMAIL=tester@example.com
export GIT_COMMITTER_NAME=Tester GIT_COMMITTER_EMAIL=tester@example.com
zero_sha=0000000000000000000000000000000000000000
failures=0

write_file() {
  local path="$1"
  mkdir -p "$(dirname "${path}")"
  printf '%s\n' "${@:2}" > "${path}"
}

commit_all() {
  local repository="$1" message="$2"
  git -C "${repository}" add --all
  git -C "${repository}" commit --quiet --message="${message}"
  git -C "${repository}" rev-parse HEAD
}

record_result() {
  local case_name="$1" status="$2" expected_status="$3" output="$4" expected_output="$5"
  if [[ "${status}" -eq "${expected_status}" && "${output}" == "${expected_output}" ]]; then
    echo "pass case=${case_name}"
  else
    echo "fail case=${case_name} status=${status} expected_status=${expected_status}"
    echo "${output}"
    failures=$((failures + 1))
  fi
}

expect_scan() {
  local case_name="$1" tree="$2" names_file="$3" expected_status="$4" expected_output="$5"
  local status=0 output
  if [[ -n "${names_file}" ]]; then
    output="$(cd "${tree}" && PREBURN_PRIVATE_NAMES_FILE="${names_file}" "${check_script}" "${@:6}" 2> /dev/null)" || status=$?
  else
    output="$(cd "${tree}" && env -u PREBURN_PRIVATE_NAMES_FILE "${check_script}" 2> /dev/null)" || status=$?
  fi
  record_result "${case_name}" "${status}" "${expected_status}" "${output}" "${expected_output}"
}

expect_push() {
  local case_name="$1" repository="$2" expected_status="$3" expected_output="$4"
  local status=0 output
  output="$(
    cd "${repository}"
    for ref_line in "${@:5}"; do
      printf '%s\n' "${ref_line}"
    done | PATH="${fake_make_directory}:${PATH}" PREBURN_PRIVATE_NAMES_FILE="${names_file}" \
      "${pre_push_hook}" origin https://example.com/repository.git 2> /dev/null
  )" || status=$?
  record_result "${case_name}" "${status}" "${expected_status}" "${output}" "${expected_output}"
}

names_file="${workspace}/names.txt"
write_file "${names_file}" '# fictional names for tests' '' 'Zorblax' '  Quux Industries  ' 'word:Vex' 'Blip+' 'Frob.io' 'Ölwerk'
empty_names_file="${workspace}/empty-names.txt"
write_file "${empty_names_file}" '# only comments' '' '   '
empty_word_names_file="${workspace}/empty-word-names.txt"
write_file "${empty_word_names_file}" 'Zorblax' 'word:'

fake_make_directory="${workspace}/fake-make"
mkdir -p "${fake_make_directory}"
cat > "${fake_make_directory}/make" << EOF
#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail
if [[ "\$1" != check-private-names ]]; then
  exit 3
fi
if [[ "\$#" -eq 2 ]]; then
  read -r -a revisions <<< "\${2#REVISIONS=}"
  exec "${check_script}" --revisions "\${revisions[@]}"
fi
exec "${check_script}"
EOF
chmod +x "${fake_make_directory}/make"

clean_tree="${workspace}/clean"
write_file "${clean_tree}/README.md" 'Widgets for everyone' 'vexation and convex shapes' 'évex' 'Blip alone' 'frobXio'
write_file "${clean_tree}/.gitignore" 'private/' '.venv/' 'build/'
write_file "${clean_tree}/private/notes.md" 'Zorblax'
write_file "${clean_tree}/.venv/lib/module.py" 'Zorblax'
write_file "${clean_tree}/build/output.txt" 'Zorblax'
write_file "${clean_tree}/vendor/library/.git/config" 'Zorblax'
git -C "${clean_tree}" init --quiet --initial-branch=main

hit_tree="${workspace}/hit"
write_file "${hit_tree}/catalog/quux_industries/plan.yaml" 'plan: basic'
write_file "${hit_tree}/docs/zorblax-migration.md" 'Migration guide'
write_file "${hit_tree}/docs/guide.md" 'Guide' '' 'Built for ZORBLAX customers.'
write_file "${hit_tree}/docs/other.md" 'Zorblaxian'
write_file "${hit_tree}/src/main.py" '"""Entry point."""' '# quux industries contract'
write_file "${hit_tree}/src/forms.py" \
  '"""Forms."""' \
  'zorblax_client = 1' \
  'ZORBLAX_TOKEN=x' \
  'zorblax2 build' \
  'see Quux-Industries' \
  'QuuxIndustries' \
  'quux_industries' \
  'https://quuxindustries.example' \
  'Quux  Industries' \
  $'Quux\tIndustries' \
  'vex_client' \
  'vex2' \
  '(vex)' \
  'Blip+ edition' \
  'frob.io' \
  'ÖLWERK'
git -C "${hit_tree}" init --quiet --initial-branch=main

plain_directory="${workspace}/plain"
write_file "${plain_directory}/README.md" 'Widgets for everyone'

repository="${workspace}/repository"
write_file "${repository}/tracked.md" 'hello' 'Zorblax'
write_file "${repository}/deleted.md" 'Zorblax'
write_file "${repository}/untracked.md" 'Quux Industries'
write_file "${repository}/ignored.md" 'Zorblax'
write_file "${repository}/.gitignore" 'ignored.md'
git -C "${repository}" init --quiet --initial-branch=main
git -C "${repository}" add tracked.md deleted.md .gitignore
rm "${repository}/deleted.md"

broken_repository="${workspace}/broken-repository"
write_file "${broken_repository}/README.md" 'Zorblax'
write_file "${broken_repository}/.git" 'gitdir: /nonexistent'

history_repository="${workspace}/history-repository"
write_file "${history_repository}/README.md" 'History'
write_file "${history_repository}/notes.md" 'Built for Zorblax.'
write_file "${history_repository}/quux_industries.yaml" 'plan: basic'
git -C "${history_repository}" init --quiet --initial-branch=main
name_added_commit="$(commit_all "${history_repository}" 'add notes')"
write_file "${history_repository}/notes.md" 'Built for customers.'
rm "${history_repository}/quux_industries.yaml"
name_removed_commit="$(commit_all "${history_repository}" 'reword notes')"
write_file "${history_repository}/docs.md" 'Documentation'
clean_commit="$(commit_all "${history_repository}" 'add docs')"
history_hits="${name_added_commit}:quux_industries.yaml"$'\n'"${name_added_commit}:notes.md:1"

expect_scan clean-tree "${clean_tree}" "${names_file}" 0 ''
expect_scan hit-tree "${hit_tree}" "${names_file}" 1 "$(printf '%s\n' \
  catalog/quux_industries/plan.yaml docs/zorblax-migration.md docs/guide.md:3 docs/other.md:1 \
  src/forms.py:{2..16} src/main.py:2)"
expect_scan missing-names-file "${clean_tree}" "${workspace}/missing.txt" 2 ''
expect_scan unset-names-file "${clean_tree}" '' 2 ''
expect_scan names-file-without-entries "${clean_tree}" "${empty_names_file}" 2 ''
expect_scan names-file-with-empty-word-entry "${clean_tree}" "${empty_word_names_file}" 2 ''
expect_scan unknown-argument "${clean_tree}" "${names_file}" 2 '' --unknown
expect_scan not-a-repository "${plain_directory}" "${names_file}" 2 ''
expect_scan git-repository "${repository}" "${names_file}" 1 $'tracked.md:2\nuntracked.md:1'
expect_scan git-listing-fails "${broken_repository}" "${names_file}" 2 ''
expect_scan revisions-name-removed-later "${history_repository}" "${names_file}" 1 "${history_hits}" \
  --revisions "${name_removed_commit}" --not --remotes
expect_scan revisions-clean-range "${history_repository}" "${names_file}" 0 '' \
  --revisions "${clean_commit}" --not "${name_removed_commit}"
expect_scan revisions-full-history "${history_repository}" "${names_file}" 1 "${history_hits}" --revisions --all
expect_scan revisions-without-arguments "${history_repository}" "${names_file}" 2 '' --revisions
expect_scan revisions-unknown-revision "${history_repository}" "${names_file}" 2 '' --revisions refs/heads/missing
expect_push push-new-branch-with-removed-name "${history_repository}" 1 "${history_hits}" \
  "refs/heads/main ${clean_commit} refs/heads/main ${zero_sha}"
expect_push push-clean-update "${history_repository}" 0 '' \
  "refs/heads/main ${clean_commit} refs/heads/main ${name_removed_commit}"
expect_push push-branch-deletion "${history_repository}" 0 '' \
  "(delete) ${zero_sha} refs/heads/old ${name_added_commit}"
expect_push push-scans-every-ref "${history_repository}" 1 "${history_hits}" \
  "refs/heads/main ${clean_commit} refs/heads/main ${name_removed_commit}" \
  "refs/heads/feature ${name_removed_commit} refs/heads/feature ${zero_sha}"
expect_push push-scans-working-tree "${repository}" 1 $'tracked.md:2\nuntracked.md:1'

if [[ "${failures}" -ne 0 ]]; then
  echo "failures=${failures}"
  exit 1
fi
