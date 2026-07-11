#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

if [[ -n "${CLANG_FORMAT:-}" ]]; then
    clang_format=${CLANG_FORMAT}
elif command -v clang-format >/dev/null 2>&1; then
    clang_format=$(command -v clang-format)
elif command -v xcrun >/dev/null 2>&1 \
    && xcrun --find clang-format >/dev/null 2>&1; then
    clang_format=$(xcrun --find clang-format)
else
    echo "ERROR: clang-format is required for the C/C++ style check" >&2
    exit 1
fi

formatter_major=$(
    "$clang_format" --version \
        | sed -E 's/.*version ([0-9]+).*/\1/'
)
if [[ ! "$formatter_major" =~ ^[0-9]+$ ]] \
    || ((formatter_major < 15)); then
    echo "ERROR: clang-format 15 or newer is required" >&2
    exit 1
fi

cd "$repo_root"

source_patterns=(
    'include/*.h' 'include/**/*.h' \
    'src/*.c' 'src/*.cc' 'src/*.cpp' 'src/*.h' 'src/*.hpp' \
    'src/**/*.c' 'src/**/*.cc' 'src/**/*.cpp' 'src/**/*.h' 'src/**/*.hpp' \
    'test/*.c' 'test/*.cc' 'test/*.cpp' 'test/*.h' 'test/*.hpp' \
    'tools/*.c' 'tools/*.cc' 'tools/*.cpp' 'tools/*.h' 'tools/*.hpp' \
    'tools/**/*.c' 'tools/**/*.cc' 'tools/**/*.cpp' \
    'tools/**/*.h' 'tools/**/*.hpp'
)

source_files()
{
    git ls-files -z -- "${source_patterns[@]}"
}

source_files \
    | xargs -0 "$clang_format" --dry-run --Werror --style=file

line_violations=$(
    source_files \
        | xargs -0 awk '
            length($0) > 120 || index($0, "\t") > 0 {
                print FILENAME ":" FNR ":" $0
            }
        '
)
if [[ -n "$line_violations" ]]; then
    echo "ERROR: lines exceed 120 columns or contain TAB characters" >&2
    echo "$line_violations" >&2
    exit 1
fi

echo "C/C++ style check passed"
