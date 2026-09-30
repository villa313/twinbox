# Contributing

Issues and pull requests are welcome.

- Build and test: `dotnet build Twinbox.slnx -c Release` and `dotnet test --solution Twinbox.slnx -c Release`.
  Integration tests start databases and brokers with Testcontainers, so Docker must be running.
- Warnings are errors, and package versions live in `Directory.Packages.props`.
- Public API changes must be recorded: run `eng/update-public-api.sh` and include the `PublicAPI.Unshipped.txt` diff.
- Keep changes focused and covered by tests; new stores can reuse `OutboxStoreConformance` from Twinbox.Testing.
