# BenchFS Runtime Profiles

Each `*.toml` file in this directory is a flat env-var preset for benchfs
client + server processes. The `sirius-io500.sh` submit script picks one
up automatically when `BENCHFS_PROFILE=<path>` is set in the calling
shell.

## Usage

```bash
# Use the validated locusta defaults
BENCHFS_PROFILE=jobs/profiles/locusta_default.toml \
  LABEL=loc-baseline SELECT_NODES=80 BEST_PPN=2 BEST_TRANSFER=64m BEST_BLOCK=2g \
  bash jobs/io500/sirius-io500.sh

# Override a single key for a sweep variant — caller env wins over the profile
BENCHFS_PROFILE=jobs/profiles/locusta_default.toml \
  BENCHFS_PREWARM_CONCURRENCY=8 LABEL=loc-pw8 ... \
  bash jobs/io500/sirius-io500.sh

# Combine profiles by sourcing the loader twice (second source wins for
# unset keys; already-set values are kept)
source jobs/profiles/load_profile.sh jobs/profiles/locusta_default.toml
source jobs/profiles/load_profile.sh jobs/profiles/ucx_diagnostic.toml
```

## Override precedence

`load_profile.sh` checks `${!key:-}` before exporting — if the variable
is **already set non-empty** in the calling shell, the TOML value is
**skipped**. So:

1. Caller env vars (highest priority)
2. TOML profile defaults

This lets you keep validated defaults in TOML and tweak per-run by just
setting variables on the command line.

## File format

A TOML-look-alike subset that bash can parse without `tomllib`:

```toml
# Comment line.
[section_header]   # purely visual — keys keep their full env name
KEY_NAME = "value"      # quoted strings have the quotes stripped
NUMERIC_KEY = 12345     # numbers pass through as-is
BOOL_KEY = 1            # 1/0; treat as int in the consumer
```

Each non-comment, non-section line becomes `export KEY=value`.
Trailing `# inline-comment` is stripped from unquoted values.

## Profiles

| File | Purpose |
|---|---|
| `locusta_default.toml` | Validated 10 phys × ppn=2 baseline (job 20382/20238 recipe). Use this for steady-state runs. |
| `ucx_diagnostic.toml` | UCX transport + full `AM_SEND_TIMING` / `AM_SEND_BREAKDOWN` instrumentation. Use for performance investigation. |

Add new profiles by copying one of the above and editing the key/value
lines. The loader is forgiving about extra comments and blank lines.
