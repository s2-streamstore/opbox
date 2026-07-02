#!/usr/bin/env bash
set -euo pipefail

: "${S2_ACCESS_TOKEN:?S2_ACCESS_TOKEN is required}"
: "${S2_BASIN:?S2_BASIN is required}"

command -v ob >/dev/null || {
  echo "ob must be on PATH" >&2
  exit 1
}

if [[ -z "${OPBOX_CONFIG_DIR:-}" ]]; then
  OPBOX_CONFIG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/opbox-config.XXXXXX")"
  export OPBOX_CONFIG_DIR
fi

if [[ -z "${OPBOX_SMOKE_ROOT:-}" ]]; then
  smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/opbox-s2-smoke.XXXXXX")"
else
  smoke_root="${OPBOX_SMOKE_ROOT}"
fi

source_dir="${smoke_root}/source"
clone_dir="${smoke_root}/clone"
expected_notes="${smoke_root}/expected-notes.txt"
expected_nested="${smoke_root}/expected-nested.txt"
mkdir -p "${source_dir}" "${clone_dir}"

dump_logs() {
  local status=$?
  set +e
  ob stop "${source_dir}" >/dev/null 2>&1
  if [[ "${status}" -ne 0 ]]; then
    echo "::group::opbox daemon logs"
    find "${smoke_root}" -path "*/.opbox/daemon.log" -print -exec sed -n '1,240p' {} \;
    echo "::endgroup::"
  fi
  exit "${status}"
}
trap dump_logs EXIT

ob config set access-token "${S2_ACCESS_TOKEN}"
ob config set default-basin "${S2_BASIN}"
unset S2_ACCESS_TOKEN
unset S2_BASIN

init_output="$(ob init "${source_dir}")"
printf '%s\n' "${init_output}"
workspace_id="$(awk '/your workspace is:/ {print $4}' <<<"${init_output}")"
if [[ -z "${workspace_id}" ]]; then
  echo "failed to parse workspace id from ob init output" >&2
  exit 1
fi
# Exact-field match: init output can also contain a `--cipher <key> \` line
# inside the printed clone command, which a bare /cipher/ regex would match.
cipher="$(awk '$1 == "cipher" {print $2}' <<<"${init_output}")"
if [[ -z "${cipher}" ]]; then
  echo "failed to parse cipher from ob init output" >&2
  exit 1
fi

ob start "${source_dir}"
baseline_cursor="$(ob status "${source_dir}" | awk '/stable cursor/ {print $NF}')"
if [[ ! "${baseline_cursor}" =~ ^[0-9]+$ ]]; then
  echo "failed to read numeric baseline stable cursor" >&2
  ob status "${source_dir}" >&2 || true
  exit 1
fi

cat >"${expected_notes}" <<EOF
hello from GitHub CI
workspace ${workspace_id}
source file observed through clone
EOF
mkdir -p "${source_dir}/nested"
cat >"${expected_nested}" <<EOF
nested file from GitHub CI
workspace ${workspace_id}
EOF
cp "${expected_notes}" "${source_dir}/notes.txt"
cp "${expected_nested}" "${source_dir}/nested/info.txt"

last_cursor="${baseline_cursor}"
expected_cursor=$((baseline_cursor + 3))
stable_polls=0
deadline=$((SECONDS + 90))
while ((SECONDS < deadline)); do
  status_output="$(ob status "${source_dir}")"
  cursor="$(awk '/stable cursor/ {print $NF}' <<<"${status_output}")"
  connectivity="$(awk '/connectivity/ {$1=""; sub(/^ +/, ""); print}' <<<"${status_output}")"

  if [[ "${connectivity}" == online* && "${cursor}" =~ ^[0-9]+$ && "${cursor}" -ge "${expected_cursor}" ]]; then
    if [[ "${cursor}" == "${last_cursor}" ]]; then
      stable_polls=$((stable_polls + 1))
    else
      stable_polls=0
      last_cursor="${cursor}"
    fi

    if ((stable_polls >= 5)); then
      break
    fi
  else
    stable_polls=0
  fi

  sleep 1
done

if ((stable_polls < 5)); then
  echo "timed out waiting for source daemon to sync writes through cursor ${expected_cursor}" >&2
  ob status "${source_dir}" >&2 || true
  exit 1
fi

ob stop "${source_dir}"
ob clone --workspace "${workspace_id}" --cipher "${cipher}" "${clone_dir}"

cmp "${expected_notes}" "${clone_dir}/notes.txt"
cmp "${expected_nested}" "${clone_dir}/nested/info.txt"
