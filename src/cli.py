import click
from pathlib import Path
from typing import Optional

from workload_generator import (
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_IDLE_SECONDS,
    DEFAULT_MAX_DURATION_SECONDS,
    DEFAULT_MIN_DURATION_SECONDS,
    DEFAULT_SEED,
    DEFAULT_WARMUP_SECONDS,
    WorkloadGenerator,
    load_generator_config,
)

DEFAULT_TEMPLATE_PATH = "assets/cpu-burn-template.yaml"
DEFAULT_GENERATOR_CONFIG_PATH = "assets/generator-config.yaml"


@click.group()
def cli():
    pass


@cli.command("batch", help="Launch multiple CPU-burn Kubernetes jobs in batches with randomized steady durations")
@click.option("--template", required=True, default=DEFAULT_TEMPLATE_PATH, help="Path to Job YAML template")
@click.option("--namespace", default="default", required=True, help="Kubernetes namespace")
@click.option("--job-name", default="demo", help="Base name for the jobs")
@click.option("--total-jobs", default=20, required=True, type=int, help="Total number of jobs to launch")
@click.option("--batch-size", default=5, required=True, type=int, help="Number of jobs to launch in each batch")
@click.option("--wait-seconds", default=300, required=True, type=int, help="Seconds to wait between batches")
@click.option("--delete-after-seconds", default=300, required=True, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it")
@click.option("--status-poll-seconds", default=10, required=True, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup")
@click.option("--seed", default=DEFAULT_SEED, type=int, help="Fixed seed used for reproducible duration sampling and workload initialization")
@click.option("--idle-seconds", default=DEFAULT_IDLE_SECONDS, type=click.IntRange(min=0), help="Idle sleep before CPU warm-up")
@click.option("--warmup-seconds", default=DEFAULT_WARMUP_SECONDS, type=click.IntRange(min=0), help="CPU warm-up phase duration")
@click.option("--min-duration-seconds", default=DEFAULT_MIN_DURATION_SECONDS, type=click.IntRange(min=0), help="Minimum steady CPU-burn duration, inclusive")
@click.option("--max-duration-seconds", default=DEFAULT_MAX_DURATION_SECONDS, type=click.IntRange(min=0), help="Maximum steady CPU-burn duration, inclusive")
@click.option("--cooldown-seconds", default=DEFAULT_COOLDOWN_SECONDS, type=click.IntRange(min=0), help="Idle cooldown sleep after the CPU-burn phase")
def batch(
    template: str,
    namespace: str,
    job_name: str,
    total_jobs: int,
    batch_size: int,
    wait_seconds: int,
    delete_after_seconds: int,
    status_poll_seconds: int,
    seed: int,
    idle_seconds: int,
    warmup_seconds: int,
    min_duration_seconds: int,
    max_duration_seconds: int,
    cooldown_seconds: int,
) -> int:
    if min_duration_seconds > max_duration_seconds:
        raise click.BadParameter("--min-duration-seconds must be less than or equal to --max-duration-seconds")

    generator = WorkloadGenerator(Path(template))
    generator.run_batch(
        namespace=namespace,
        job_name=job_name,
        total_jobs=total_jobs,
        batch_size=batch_size,
        wait_seconds=wait_seconds,
        delete_after_seconds=delete_after_seconds,
        status_poll_seconds=status_poll_seconds,
        seed=seed,
        idle_seconds=idle_seconds,
        warmup_seconds=warmup_seconds,
        min_duration_seconds=min_duration_seconds,
        max_duration_seconds=max_duration_seconds,
        cooldown_seconds=cooldown_seconds,
    )
    return 0


@cli.command("poisson", help="Launch CPU-burn Kubernetes jobs according to one or more Poisson workload stages with randomized steady durations")
@click.option("--generator-config", default=None, type=click.Path(exists=True, dir_okay=False), help=f"Path to generator YAML config, e.g. {DEFAULT_GENERATOR_CONFIG_PATH}")
@click.option("--template", default=DEFAULT_TEMPLATE_PATH, help="Path to Job YAML template. Ignored when --generator-config is used.")
@click.option("--namespace", default="default", help="Kubernetes namespace. Ignored when --generator-config is used.")
@click.option("--job-name", default="demo", help="Base name for the jobs. Ignored when --generator-config is used.")
@click.option("--total-jobs", default=20, type=int, help="Total number of jobs to launch. Ignored when --generator-config is used.")
@click.option("--iat-seconds", default=30.0, type=float, help="Mean inter-arrival time in seconds for the Poisson process. Ignored when --generator-config is used.")
@click.option("--delete-after-seconds", default=300, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it. Ignored when --generator-config is used.")
@click.option("--status-poll-seconds", default=10, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup. Ignored when --generator-config is used.")
@click.option("--seed", default=DEFAULT_SEED, type=int, help="Fixed seed for reproducible Poisson arrivals, duration sampling, and CPU-burn initialization. Ignored when --generator-config is used.")
@click.option("--idle-seconds", default=DEFAULT_IDLE_SECONDS, type=click.IntRange(min=0), help="Idle sleep before CPU warm-up. Ignored when --generator-config is used.")
@click.option("--warmup-seconds", default=DEFAULT_WARMUP_SECONDS, type=click.IntRange(min=0), help="CPU warm-up phase duration. Ignored when --generator-config is used.")
@click.option("--min-duration-seconds", default=DEFAULT_MIN_DURATION_SECONDS, type=click.IntRange(min=0), help="Minimum steady CPU-burn duration, inclusive. Ignored when --generator-config is used.")
@click.option("--max-duration-seconds", default=DEFAULT_MAX_DURATION_SECONDS, type=click.IntRange(min=0), help="Maximum steady CPU-burn duration, inclusive. Ignored when --generator-config is used.")
@click.option("--cooldown-seconds", default=DEFAULT_COOLDOWN_SECONDS, type=click.IntRange(min=0), help="Idle cooldown sleep after the CPU-burn phase. Ignored when --generator-config is used.")
def poisson(
    generator_config: Optional[str],
    template: str,
    namespace: str,
    job_name: str,
    total_jobs: int,
    iat_seconds: float,
    delete_after_seconds: int,
    status_poll_seconds: int,
    seed: int,
    idle_seconds: int,
    warmup_seconds: int,
    min_duration_seconds: int,
    max_duration_seconds: int,
    cooldown_seconds: int,
) -> int:
    if generator_config:
        generator_config_path = Path(generator_config)
        config_data = load_generator_config(generator_config_path)
        template_path = Path(config_data.template)
        if not template_path.is_absolute():
            template_path = generator_config_path.parent / template_path

        generator = WorkloadGenerator(template_path)
        generator.run_poisson_config(config_data)
        return 0

    if min_duration_seconds > max_duration_seconds:
        raise click.BadParameter("--min-duration-seconds must be less than or equal to --max-duration-seconds")

    generator = WorkloadGenerator(Path(template))
    generator.run_poisson(
        namespace=namespace,
        job_name=job_name,
        total_jobs=total_jobs,
        iat_seconds=iat_seconds,
        delete_after_seconds=delete_after_seconds,
        status_poll_seconds=status_poll_seconds,
        seed=seed,
        idle_seconds=idle_seconds,
        warmup_seconds=warmup_seconds,
        min_duration_seconds=min_duration_seconds,
        max_duration_seconds=max_duration_seconds,
        cooldown_seconds=cooldown_seconds,
    )
    return 0


if __name__ == "__main__":
    cli()
