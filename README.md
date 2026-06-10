# K8S Workload Generator

Simple Kubernetes workload generator that:

- Creates a batch of jobs with a specified template and parameters.
- Creates jobs following a Poisson process with one or more configurable inter-arrival-time stages.
- Randomizes the Fibonacci workload size per job with reproducible `min_N` / `max_N` sampling when a seed is supplied.

# Softwares

- Python 3.8.10

# Sample Usage

## Batch jobs

```bash
python3 src/cli.py batch \
    --template assets/fibonacci-template.yaml \
    --namespace exp --job-name demo \
    --total-jobs 20 --batch-size 5 --wait-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30 \
    --seed 42 --min-n 37 --max-n 42
```

## Poisson jobs with CLI options

This keeps the original single-stage Poisson usage and adds random Fibonacci `N` selection.
With the same `--seed`, the Poisson inter-arrival sequence and the per-job `FIB_N` sequence are reproducible.

```bash
python3 src/cli.py poisson \
    --template assets/fibonacci-template.yaml \
    --namespace exp --job-name demo \
    --total-jobs 20 --iat-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30 \
    --seed 42 --min-n 37 --max-n 42
```

## Poisson jobs with a YAML config

The `poisson` command can also read a config YAML file, following the staged workload style used by `k8s-spark-workload-generator`:

```bash
python3 src/cli.py poisson --generator-config assets/generator-config.yaml
```

Example config:

```yaml
template: fibonacci-template.yaml
namespace: exp
job_name: demo
seed: 42
delete_after_seconds: 300
status_poll_seconds: 10
min_N: 37
max_N: 42
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

Each stage launches `amount` jobs. Inter-arrival times inside a stage are sampled from an exponential distribution with mean `iat_seconds`. Each job receives a `FIB_N` environment variable sampled uniformly from `[min_N, max_N]` using the same seeded random generator.
