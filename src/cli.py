import click
from pathlib import Path
from typing import Optional

from workload_generator import WorkloadGenerator, load_generator_config

DEFAULT_TEMPLATE_PATH = "assets/fibonacci-template.yaml"
DEFAULT_GENERATOR_CONFIG_PATH = "assets/generator_config.yaml"


@click.group()
def cli():
    pass


@cli.command("batch", help="Launch multiple Kubernetes jobs in batch")
@click.option("--template", required=True, default=DEFAULT_TEMPLATE_PATH, help="Path to Job YAML template")
@click.option("--namespace", default="default", required=True, help="Kubernetes namespace")
@click.option("--job-name", default="demo", help="Base name for the jobs")
@click.option("--total-jobs", default=20, required=True, type=int, help="Total number of jobs to launch")
@click.option("--batch-size", default=5, required=True, type=int, help="Number of jobs to launch in each batch")
@click.option("--wait-seconds", default=300, required=True, type=int, help="Seconds to wait between batches")
@click.option("--delete-after-seconds", default=300, required=True, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it")
@click.option("--status-poll-seconds", default=10, required=True, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup")
@click.option("--seed", default=None, type=int, help="Random seed for reproducible Fibonacci N selection (optional)")
@click.option("--min-n", "--min-N", "min_N", default=42, type=click.IntRange(min=0), help="Minimum Fibonacci N to use for each job")
@click.option("--max-n", "--max-N", "max_N", default=42, type=click.IntRange(min=0), help="Maximum Fibonacci N to use for each job")
def batch(
    template: str,
    namespace: str,
    job_name: str,
    total_jobs: int,
    batch_size: int,
    wait_seconds: int,
    delete_after_seconds: int,
    status_poll_seconds: int,
    seed: Optional[int],
    min_N: int,
    max_N: int,
) -> int:
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
        min_N=min_N,
        max_N=max_N,
    )
    return 0


@cli.command("poisson", help="Launch Kubernetes jobs according to one or more Poisson workload stages")
@click.option("--generator-config", default=None, type=click.Path(exists=True, dir_okay=False), help=f"Path to generator YAML config, e.g. {DEFAULT_GENERATOR_CONFIG_PATH}")
@click.option("--template", default=DEFAULT_TEMPLATE_PATH, help="Path to Job YAML template. Ignored when --generator-config is used.")
@click.option("--namespace", default="default", help="Kubernetes namespace. Ignored when --generator-config is used.")
@click.option("--job-name", default="demo", help="Base name for the jobs. Ignored when --generator-config is used.")
@click.option("--total-jobs", default=20, type=int, help="Total number of jobs to launch. Ignored when --generator-config is used.")
@click.option("--iat-seconds", default=30.0, type=float, help="Mean inter-arrival time in seconds for the Poisson process. Ignored when --generator-config is used.")
@click.option("--delete-after-seconds", default=300, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it. Ignored when --generator-config is used.")
@click.option("--status-poll-seconds", default=10, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup. Ignored when --generator-config is used.")
@click.option("--seed", default=None, type=int, help="Random seed for reproducible Poisson arrivals and Fibonacci N selection. Ignored when --generator-config is used.")
@click.option("--min-n", "--min-N", "min_N", default=42, type=click.IntRange(min=0), help="Minimum Fibonacci N to use for each job. Ignored when --generator-config is used.")
@click.option("--max-n", "--max-N", "max_N", default=42, type=click.IntRange(min=0), help="Maximum Fibonacci N to use for each job. Ignored when --generator-config is used.")
def poisson(
    generator_config: Optional[str],
    template: str,
    namespace: str,
    job_name: str,
    total_jobs: int,
    iat_seconds: float,
    delete_after_seconds: int,
    status_poll_seconds: int,
    seed: Optional[int],
    min_N: int,
    max_N: int,
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

    generator = WorkloadGenerator(Path(template))
    generator.run_poisson(
        namespace=namespace,
        job_name=job_name,
        total_jobs=total_jobs,
        iat_seconds=iat_seconds,
        delete_after_seconds=delete_after_seconds,
        status_poll_seconds=status_poll_seconds,
        seed=seed,
        min_N=min_N,
        max_N=max_N,
    )
    return 0


if __name__ == "__main__":
    cli()
