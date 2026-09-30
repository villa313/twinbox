# Contributing

Issues and pull requests are welcome.

- Build and test: `dotnet build Twinbox.slnx -c Release` and `dotnet test --solution Twinbox.slnx -c Release`.
  Integration tests start databases and brokers with Testcontainers, so Docker must be running.
- Warnings are errors, and package versions live in `Directory.Packages.props`.
- Public API changes must be recorded: run `eng/update-public-api.sh` and include the `PublicAPI.Unshipped.txt` diff.
- Keep changes focused and covered by tests; new stores can reuse `OutboxStoreConformance` from Twinbox.Testing.

## Versioning and releases

Versions follow [Semantic Versioning](https://semver.org) and come from git tags; there is no version number in the
project files. A tag `vX.Y.Z` on `main` publishes that version to NuGet. Untagged builds are versioned as the next
patch's prerelease (for example `1.0.1-alpha.0.3`) and are never published.

- **Patch** (`v1.0.1`): bug fixes only; no public API changes.
- **Minor** (`v1.1.0`): new features and additive public API; `PublicAPI.Unshipped.txt` has only additions.
- **Major** (`v2.0.0`): anything that removes or changes public API or breaks existing behaviour.

Before a release, move the lines from each `PublicAPI.Unshipped.txt` into `PublicAPI.Shipped.txt`.

To release, run the `release` workflow on `main` from the Actions tab, or `gh workflow run release.yml -f bump=patch`
(`minor`, `major`). It checks that CI passed on that commit and that no unshipped API is left, tags the next version and
publishes it. Pushing a tag yourself (`git tag -a v1.2.0-rc.1 -m ...`) still works, and is how to publish a prerelease.

`dotnet pack` compares every package with its 1.0.0 release and fails on breaking changes. After a major release, raise
`PackageValidationBaselineVersion` in `src/Directory.Build.props` to the new major version.
