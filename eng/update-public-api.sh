#!/usr/bin/env bash
# Syncs each project's PublicAPI.Unshipped.txt with the build: adds undeclared symbols (RS0016), drops removed ones (RS0017).
set -euo pipefail
cd "$(dirname "$0")/.."
dotnet build Twinbox.slnx -c Release 2>&1 | python3 -c '
import re, sys, collections
added = collections.defaultdict(set)
removed = collections.defaultdict(set)
for line in sys.stdin:
    m = re.search(r"(RS001[67]): Symbol \x27(.+?)\x27 is .*\[(.+?\.csproj)", line)
    if m:
        (added if m.group(1) == "RS0016" else removed)[m.group(3)].add(m.group(2))
for project in set(added) | set(removed):
    path = project.rsplit("/", 1)[0] + "/PublicAPI.Unshipped.txt"
    existing = set(l for l in open(path).read().splitlines() if l and not l.startswith("#"))
    lines = sorted((existing | added[project]) - removed[project])
    open(path, "w").write("#nullable enable\n" + "\n".join(lines) + "\n")
    print(f"{path}: +{len(added[project] - existing)} -{len(removed[project] & existing)}")
'
