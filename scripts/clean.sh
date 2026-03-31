#!/usr/bin/env bash
# clean.sh — remove cmake build directories so the next build.bat/.sh runs a
# full fresh configure.  Useful when switching MSYS2 environments or after a
# dependency upgrade that changes ABI.

set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_DIRS=(
  "${ROOT_DIR}/build"
  "${ROOT_DIR}/build-coverage"
  "${ROOT_DIR}/build-debug"
)

removed=0
for d in "${BUILD_DIRS[@]}"; do
  if [[ -d "${d}" ]]; then
    echo "Removing: ${d}"
    rm -rf "${d}"
    removed=$((removed + 1))
  fi
done

if [[ ${removed} -eq 0 ]]; then
  echo "Nothing to clean."
else
  echo "Done. Run build.bat (Windows) or bash scripts/build.sh (Linux/macOS) to rebuild."
fi
