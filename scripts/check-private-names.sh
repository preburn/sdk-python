#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail -o errtrace
trap 'exit 2' ERR
export LC_ALL=C.UTF-8

word_prefix="word:"
word_separator='[[:space:]_-]*'
letter_boundary_start='(^|[^[:alpha:]])'
letter_boundary_end='([^[:alpha:]]|$)'

scan_working_tree() {
  git ls-files -z --cached --others --exclude-standard > "${listed_files}"
  sort -z -o "${listed_files}" "${listed_files}"

  local scanned_files=() path line_number
  while IFS= read -r -d '' path; do
    if [[ -f "${path}" ]]; then
      scanned_files+=("${path}")
    fi
  done < "${listed_files}"
  if [[ "${#scanned_files[@]}" -eq 0 ]]; then
    return
  fi

  printf '%s\n' "${scanned_files[@]}" | grep "${match_options[@]}" || [[ $? -eq 1 ]]
  grep "${match_options[@]}" -l --null -- "${scanned_files[@]}" > "${matched_files}" || [[ $? -eq 1 ]]
  while IFS= read -r -d '' path; do
    grep "${match_options[@]}" -n -- "${path}" | cut -d : -f 1 | while IFS= read -r line_number; do
      printf '%s:%s\n' "${path}" "${line_number}"
    done
  done < "${matched_files}"
}

scan_revisions() {
  local commit path line_number
  git rev-list "$@" > "${commits_file}"
  while IFS= read -r commit; do
    git ls-tree -r -z --name-only "${commit}" > "${listed_files}"
    tr '\0' '\n' < "${listed_files}" | grep "${match_options[@]}" | sed "s/^/${commit}:/" || [[ $? -eq 1 ]]
    git grep "${match_options[@]}" --files-with-matches --null "${commit}" > "${matched_files}" || [[ $? -eq 1 ]]
    while IFS= read -r -d '' path; do
      path="${path#"${commit}:"}"
      git grep "${match_options[@]}" --line-number -h "${commit}" -- ":(literal)${path}" \
        | cut -d : -f 1 | while IFS= read -r line_number; do
          printf '%s:%s:%s\n' "${commit}" "${path}" "${line_number}"
        done
    done < "${matched_files}"
  done < "${commits_file}"
}

if [[ $# -gt 0 && ("$1" != --revisions || $# -lt 2) ]]; then
  echo "usage: scripts/check-private-names.sh [--revisions <git rev-list arguments>...]" >&2
  exit 2
fi
if [[ -z "${PREBURN_PRIVATE_NAMES_FILE:-}" ]]; then
  echo "PREBURN_PRIVATE_NAMES_FILE is not set" >&2
  exit 2
fi
if [[ ! -f "${PREBURN_PRIVATE_NAMES_FILE}" ]]; then
  echo "names file not found path=${PREBURN_PRIVATE_NAMES_FILE}" >&2
  exit 2
fi

names="$(sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e '/^#/d' -e '/^$/d' "${PREBURN_PRIVATE_NAMES_FILE}")"
if [[ -z "${names}" ]]; then
  echo "names file has no entries path=${PREBURN_PRIVATE_NAMES_FILE}" >&2
  exit 2
fi
if grep -q -x -F "${word_prefix}" <<< "${names}"; then
  echo "names file has an empty ${word_prefix} entry path=${PREBURN_PRIVATE_NAMES_FILE}" >&2
  exit 2
fi

temporary_directory="$(mktemp -d)"
trap 'rm -rf "${temporary_directory}"' EXIT
patterns_file="${temporary_directory}/patterns"
listed_files="${temporary_directory}/listed-files"
matched_files="${temporary_directory}/matched-files"
commits_file="${temporary_directory}/commits"
hits_file="${temporary_directory}/hits"

sed -e 's/[.[\()*+?{|^$]/\\&/g' \
  -e "s/[[:space:]]\{1,\}/${word_separator}/g" \
  -e "s/^${word_prefix}\(.*\)\$/${letter_boundary_start}\1${letter_boundary_end}/" \
  <<< "${names}" > "${patterns_file}"
match_options=(-a -i -E -f "${patterns_file}")

if [[ $# -eq 0 ]]; then
  scan_working_tree > "${hits_file}"
else
  scan_revisions "${@:2}" > "${hits_file}"
fi
if [[ -s "${hits_file}" ]]; then
  cat "${hits_file}"
  echo "private names found in the lines above" >&2
  exit 1
fi
