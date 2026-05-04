# Developer Notes

The implementation uses only Python standard library modules. State lives in SQLite at `~/.local/share/lbatch/lbatch.db`; release events and generated wrappers live next to the DB under `events/` and `wrappers/`.

The core state machine is `HELD_DEPENDENCY -> QUEUED -> SUBMITTING -> REMOTE_VISIBLE -> RELEASED`, with `HELD_INVALID` for non-retryable submission errors and `FORCE_RELEASED` for manual cleanup.

The scheduler ingests local release events, releases dependency-held units, computes capacity, and submits eligible units in priority and creation order. Slurm access is isolated in `lbatch.slurm.SlurmClient` for fake command testing.
