#!/usr/bin/env bash
# Adds every symbol the build reports as undeclared (RS0016) to that project's PublicAPI.Unshipped.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
dotnet build Twinbox.slnx -c Release 2>&1 | python3 -c '
import re, sys, collections
missing = collections.defaultdict(set)
for line in sys.stdin:
    m = re.search(r"RS0016: Symbol \x27(.+?)\x27 is not part of the declared public API.*\[(.+?\.csproj)", line)
    if m:
        missing[m.group(2)].add(m.group(1))
for project, symbols in missing.items():
    path = project.rsplit("/", 1)[0] + "/PublicAPI.Unshipped.txt"
    existing = open(path).read().splitlines()
    lines = sorted(set(l for l in existing if l and not l.startswith("#")) | symbols)
    open(path, "w").write("#nullable enable\n" + "\n".join(lines) + "\n")
    print(f"{path}: +{len(symbols - set(existing))}")
'
