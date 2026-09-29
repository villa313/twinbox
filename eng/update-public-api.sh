#!/usr/bin/env bash
# Regenerates PublicAPI.Unshipped.txt entries for every packable project.
set -euo pipefail
cd "$(dirname "$0")/.."
for proj in src/*/*.csproj; do
  dotnet format analyzers "$proj" --diagnostics RS0016 RS0017 --severity info --no-restore >/dev/null 2>&1 || true
done
