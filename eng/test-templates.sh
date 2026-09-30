#!/usr/bin/env bash
# Instantiates the dotnet new template for every store and transport and builds each app with warnings as errors;
# generated test projects are run too.
# Usage: eng/test-templates.sh [directory with freshly packed Twinbox .nupkg files]
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
# Physical path: on macOS the temp folder is behind a symlink, and MSBuild would see each project under two paths.
work="$(cd "$(mktemp -d "${TMPDIR:-/tmp}/twinbox-templates.XXXXXX")" && pwd -P)"
feed="${1:-}"
jobs="${TEMPLATE_TEST_JOBS:-4}"

stores=(efcore-sqlserver efcore-postgres efcore-mysql efcore-sqlite sqlserver postgres mysql mongodb inmemory)
transports=(azure-service-bus rabbitmq kafka amazon-sqs nats redis local)

if [[ -z "$feed" ]]; then
  feed="$work/feed"
  dotnet pack "$repo_root/Twinbox.slnx" -c Release -o "$feed" -v quiet -nologo
fi
feed="$(cd "$feed" && pwd)"

template_package="$(ls "$feed"/Twinbox.Templates.*.nupkg)"
version="$(basename "$template_package" .nupkg)"
version="${version#Twinbox.Templates.}"
echo "Testing Twinbox.Templates $version"

# A private package cache and template hive, so packages of the same version from an earlier run are never reused.
export NUGET_PACKAGES="$work/packages"
hive="$work/hive"
apps="$work/apps"
mkdir -p "$apps"
cat > "$apps/nuget.config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local" value="$feed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
EOF
dotnet new install "$template_package" --debug:custom-hive "$hive" > /dev/null

cases=()
for store in "${stores[@]}"; do
  for transport in "${transports[@]}"; do
    cases+=("$store $transport")
  done
done
# Every switch at once, per store, on both target frameworks, rotating through the transports.
for i in "${!stores[@]}"; do
  transport="${transports[$((i % ${#transports[@]}))]}"
  cases+=("${stores[$i]} $transport --dashboard --webhooks --tests")
  cases+=("${stores[$i]} $transport --dashboard --webhooks --tests --framework net8.0")
done

run_case() {
  local store="$1" transport="$2"
  shift 2
  local name="${store}_${transport}"
  [[ $# -gt 0 ]] && name="${name}_full"
  [[ " $* " == *" net8.0 "* ]] && name="${name}_net8"
  local dir="$apps/$name" log="$apps/$name.log"

  if ! {
    dotnet new twinbox -n Shop -o "$dir" --store "$store" --transport "$transport" "$@" --debug:custom-hive "$hive" &&
    check_output "$dir" &&
    dotnet build "$dir/Shop.sln" -warnaserror -nologo -v quiet &&
    { [[ " $* " != *" --tests "* ]] || (cd "$dir" && dotnet test --solution Shop.sln --no-build); }
  } > "$log" 2>&1; then
    echo "FAIL $name (log: $log)"
    tail -n 30 "$log"
    return 1
  fi
  echo "ok   $name"
}

check_output() {
  local dir="$1"
  if grep -rlE 'TWINBOX_VERSION|TRANSPORT_NAME|DATABASE_CONNECTION_STRING|BROKER_CONNECTION|#if|#endif' "$dir"; then
    echo "Unprocessed template tokens in the files above."
    return 1
  fi
  if ! grep -q "Version=\"$version\"" "$dir/Shop/Shop.csproj"; then
    echo "Shop.csproj does not reference Twinbox $version."
    return 1
  fi
  python3 -m json.tool "$dir/Shop/appsettings.json" > /dev/null
}

export -f run_case check_output
export apps hive version

started=$SECONDS
failures=0
printf '%s\n' "${cases[@]}" | xargs -P "$jobs" -I{} bash -c 'run_case {}' || failures=1
echo "${#cases[@]} combinations in $((SECONDS - started)) s"

if [[ $failures -ne 0 ]]; then
  echo "Some combinations failed; generated apps are in $apps"
  exit 1
fi
rm -rf "$work"
