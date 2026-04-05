import click
from pathlib import Path

from workload_generator import WorkloadGenerator

@click.group()
def cli():
    pass

@cli.command("batch", help="Launch multiple Kubernetes jobs in batch")
@click.option("--template", required=True, help="Path to Job YAML template")
@click.option("--namespace", default="default", required=True, help="Kubernetes namespace")
@click.option("--job-name", default="demo", help="Base name for the jobs")
@click.option("--total-jobs", default=20, required=True, type=int, help="Total number of jobs to launch")
@click.option("--batch-size", default=5, required=True, type=int, help="Number of jobs to launch in each batch")
@click.option("--wait-seconds", default=300, required=True, type=int, help="Seconds to wait between batches")
@click.option("--delete-after-seconds", default=300, required=True, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it")
@click.option("--status-poll-seconds", default=10, required=True, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup")
def batch(
    template: str,
    namespace: str,
    job_name: str,
    total_jobs: int,
    batch_size: int,
    wait_seconds: int,
    delete_after_seconds: int,
    status_poll_seconds: int,
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
    )
    return 0

@cli.command("poisson", help="Launch Kubernetes jobs according to a Poisson process")
@click.option("--template", required=True, help="Path to Job YAML template")
@click.option("--namespace", default="default", required=True, help="Kubernetes namespace")
@click.option("--job-name", default="demo", help="Base name for the jobs")
@click.option("--total-jobs", default=20, required=True, type=int, help="Total number of jobs to launch")
@click.option("--iat-seconds", default=30, required=True, type=int, help="Mean inter-arrival time in seconds for the Poisson process")
@click.option("--delete-after-seconds", default=300, required=True, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it")
@click.option("--status-poll-seconds", default=10, required=True, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup")
def poisson(
    template: str,
    namespace: str,
    job_name: str,
    total_jobs: int,
    iat_seconds: int,
    delete_after_seconds: int,
    status_poll_seconds: int,
) -> int:
    generator = WorkloadGenerator(Path(template))
    generator.run_poisson(
        namespace=namespace,
        job_name=job_name,
        total_jobs=total_jobs,
        iat_seconds=iat_seconds,
        delete_after_seconds=delete_after_seconds,
        status_poll_seconds=status_poll_seconds,
    )
    return 0

if __name__ == "__main__":
    cli()
