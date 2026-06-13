# K8S Workload Generator

Simple Kubernetes workload generator that:

- Creates a batch of jobs with a specified template and parameters.
- Creates jobs following a Poisson process with one or more configurable inter-arrival-time stages.
- Runs a phased CPU-burn workload in each job.
- Samples each job's steady CPU-burn duration from a reproducible random range.
- Uses a fixed seed, default `42`, so Poisson arrivals, selected durations, and the CPU-burn initial state are reproducible.

Each launched Kubernetes Job follows this per-container shape:

```text
idle phase -> warm-up CPU phase -> steady CPU-burn phase -> cooldown phase
```

The generator samples the steady phase duration for each Job from this inclusive range:

```text
min_duration_seconds <= DURATION_SECONDS <= max_duration_seconds
```

By default this is 5 to 10 minutes:

- `min_duration_seconds`, default `300`
- `max_duration_seconds`, default `600`

The selected value is passed into the Job template as `DURATION_SECONDS`. Other phase settings are passed as environment variables too:

- `IDLE_SECONDS`, default `0`
- `WARMUP_SECONDS`, default `60`
- `DURATION_SECONDS`, selected per Job from `[300, 600]` by default
- `COOLDOWN_SECONDS`, default `0`
- `WORKLOAD_SEED`, default `42`

# Softwares

- Python 3.8.10

# Sample Usage

## Batch jobs

```bash
python3 src/cli.py batch \
    --template assets/cpu-burn-template.yaml \
    --namespace exp --job-name demo \
    --total-jobs 20 --batch-size 5 --wait-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30 \
    --seed 42 \
    --idle-seconds 0 --warmup-seconds 60 \
    --min-duration-seconds 300 --max-duration-seconds 600 \
    --cooldown-seconds 0
```

## Poisson jobs with CLI options

This keeps the original single-stage Poisson usage, but each job now runs the phased CPU burn. With the same `--seed`, the Poisson inter-arrival sequence, per-job selected durations, and per-job CPU-burn initial state are reproducible.

```bash
python3 src/cli.py poisson \
    --template assets/cpu-burn-template.yaml \
    --namespace exp --job-name demo \
    --total-jobs 20 --iat-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30 \
    --seed 42 \
    --idle-seconds 0 --warmup-seconds 60 \
    --min-duration-seconds 300 --max-duration-seconds 600 \
    --cooldown-seconds 0
```

## Poisson jobs with a YAML config

The `poisson` command can also read a config YAML file, following the staged workload style used by `k8s-spark-workload-generator`:

```bash
python3 src/cli.py poisson --generator-config assets/generator-config.yaml
```

Example config:

```yaml
template: cpu-burn-template.yaml
namespace: exp
job_name: demo
seed: 42
delete_after_seconds: 300
status_poll_seconds: 10
idle_seconds: 0
warmup_seconds: 60
min_duration_seconds: 300
max_duration_seconds: 600
cooldown_seconds: 0
workloads:
  - amount: 100
    iat_seconds: 60
  - amount: 200
    iat_seconds: 30
  - amount: 200
    iat_seconds: 15
  - amount: 200
    iat_seconds: 30
  - amount: 100
    iat_seconds: 60
```

Each stage launches `amount` jobs. Inter-arrival times inside a stage are sampled from an exponential distribution with mean `iat_seconds`. Each job receives `IDLE_SECONDS`, `WARMUP_SECONDS`, a reproducibly sampled `DURATION_SECONDS`, `COOLDOWN_SECONDS`, and `WORKLOAD_SEED` environment variables.

For backward compatibility, a config with only `duration_seconds` still works as a fixed duration by treating it as `min_duration_seconds == max_duration_seconds`.
