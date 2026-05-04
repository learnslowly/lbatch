# lbatch User Guide

Use `lbatch` as a local replacement for common `sbatch` submissions:

```bash
lbatch --partition=workq --array=1-100 job.batch
lbatch daemon --max-remote-visible 10 --foreground
```

Arrays are flattened locally. Each task is submitted to Slurm as a one-element array, so `%a` remains the intended task ID while `%A` is the remote one-element Slurm array job ID.

Local dependencies use `lb:` group IDs:

```bash
a=$(lbatch --parsable --array=1-10 job_A.batch)
lbatch --lbatch-afterany "$a" job_B.batch
```

Mixed Slurm and local dependencies are supported:

```bash
lbatch --dependency=afterany:123456:$a job_C.batch
```

Inspect state with `lbatch status`. If a wrapper event is missing after manual `scancel` or node failure, use `lbatch reconcile --apply` or `lbatch release UNIT_ID`.
