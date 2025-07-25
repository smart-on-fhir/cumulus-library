
### Checklist
- [ ] Consider if documentation in `docs/` needs to be updated
  - If you've changed the structure of a table, you may need to run `generate-md`
  - If you've added/removed `core` study fields that not in US Core, update our list of those in `core-study-details.md`
  - If you've changed the public API or a class/method that is part of the public api, update `api.md`
- [ ] If you've updated the `core` or `discovery` tables, regenerate the reference sql
- [ ] Consider if tests should be added
- [ ] Update template repo if there are changes to study configuration in `manifest.toml`
- [ ] If you're preparing to cut a pip release of a study, add that study to module_allowlist.json