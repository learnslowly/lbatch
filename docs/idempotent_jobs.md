# Idempotent Jobs

`lbatch` uses at-least-once delivery. Make each job safe to run twice.

Recommended pattern:

```bash
out="results/${SLURM_ARRAY_TASK_ID}.txt"
tmp="${out}.tmp.${SLURM_JOB_ID}"
done="${out}.done"

if [[ -s "$done" ]]; then
  exit 0
fi

run_analysis > "$tmp"
mv "$tmp" "$out"
touch "$done"
```

Use final-output checks, temporary files, atomic renames, and done markers.
