import logging
import random
import re
import time
import yaml
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple, Set, Dict
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)

DEFAULT_SEED = 42
DEFAULT_IDLE_SECONDS = 0
DEFAULT_WARMUP_SECONDS = 60
DEFAULT_DURATION_SECONDS = 900
DEFAULT_COOLDOWN_SECONDS = 0


@dataclass
class WorkloadStage:
    amount: int
    iat_seconds: float


@dataclass
class GeneratorConfig:
    template: str
    namespace: str
    job_name: str
    seed: int
    delete_after_seconds: int
    status_poll_seconds: int
    idle_seconds: int
    warmup_seconds: int
    duration_seconds: int
    cooldown_seconds: int
    workloads: List[WorkloadStage]


def _get_int(config_data: dict, key: str, default: int) -> int:
    value = config_data.get(key, default)
    if not isinstance(value, int):
        raise ValueError(f"Generator config key {key} must be an integer")
    return value


def _validate_non_negative_seconds(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer number of seconds")


def load_generator_config(config_path: Path) -> GeneratorConfig:
    with config_path.open() as f:
        config_data = yaml.safe_load(f) or {}

    workloads_data = config_data.get("workloads")
    if not isinstance(workloads_data, list) or not workloads_data:
        raise ValueError("Generator config must include a non-empty workloads list")

    workloads: List[WorkloadStage] = []
    for idx, stage in enumerate(workloads_data, start=1):
        if not isinstance(stage, dict):
            raise ValueError(f"workloads[{idx}] must be a mapping with amount and iat_seconds")
        amount = stage.get("amount")
        iat_seconds = stage.get("iat_seconds")
        if not isinstance(amount, int) or amount < 0:
            raise ValueError(f"workloads[{idx}].amount must be a non-negative integer")
        if not isinstance(iat_seconds, (int, float)) or iat_seconds <= 0:
            raise ValueError(f"workloads[{idx}].iat_seconds must be a positive number")
        workloads.append(WorkloadStage(amount=amount, iat_seconds=float(iat_seconds)))

    idle_seconds = _get_int(config_data, "idle_seconds", DEFAULT_IDLE_SECONDS)
    warmup_seconds = _get_int(config_data, "warmup_seconds", DEFAULT_WARMUP_SECONDS)
    duration_seconds = _get_int(config_data, "duration_seconds", DEFAULT_DURATION_SECONDS)
    cooldown_seconds = _get_int(config_data, "cooldown_seconds", DEFAULT_COOLDOWN_SECONDS)
    for name, value in (
        ("idle_seconds", idle_seconds),
        ("warmup_seconds", warmup_seconds),
        ("duration_seconds", duration_seconds),
        ("cooldown_seconds", cooldown_seconds),
    ):
        _validate_non_negative_seconds(name, value)

    return GeneratorConfig(
        template=config_data.get("template", "assets/cpu-burn-template.yaml"),
        namespace=config_data.get("namespace", "default"),
        job_name=config_data.get("job_name", "demo"),
        seed=_get_int(config_data, "seed", DEFAULT_SEED),
        delete_after_seconds=_get_int(config_data, "delete_after_seconds", 300),
        status_poll_seconds=_get_int(config_data, "status_poll_seconds", 10),
        idle_seconds=idle_seconds,
        warmup_seconds=warmup_seconds,
        duration_seconds=duration_seconds,
        cooldown_seconds=cooldown_seconds,
        workloads=workloads,
    )


class WorkloadGenerator:
    def __init__(self, template_path: Path):
        config.load_kube_config()
        self._batch_api = client.BatchV1Api()
        self.template_path = template_path

    @staticmethod
    def load_yaml(path: Path) -> dict:
        with path.open() as f:
            return yaml.safe_load(f)

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def now_utc_compact() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    @staticmethod
    def sanitize_k8s_name(name: str, max_length: int = 63) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9.-]+", "-", name)   # replace invalid chars, including "_"
        name = re.sub(r"^[^a-z0-9]+", "", name)     # must start with alnum
        name = re.sub(r"[^a-z0-9]+$", "", name)     # must end with alnum
        name = re.sub(r"-{2,}", "-", name)          # collapse repeated dashes
        name = name[:max_length]
        name = re.sub(r"[^a-z0-9]+$", "", name)     # re-trim after truncation
        return name or "job"

    @staticmethod
    def _validate_phase_seconds(
        idle_seconds: int,
        warmup_seconds: int,
        duration_seconds: int,
        cooldown_seconds: int,
    ) -> None:
        for name, value in (
            ("idle_seconds", idle_seconds),
            ("warmup_seconds", warmup_seconds),
            ("duration_seconds", duration_seconds),
            ("cooldown_seconds", cooldown_seconds),
        ):
            _validate_non_negative_seconds(name, value)

    @staticmethod
    def _upsert_env(container: dict, name: str, value: str) -> None:
        env = container.setdefault("env", [])
        for entry in env:
            if entry.get("name") == name:
                entry["value"] = value
                return
        env.append({"name": name, "value": value})

    def apply_overrides(
        self,
        doc: dict,
        namespace: str,
        job_name: str,
        node_name: Optional[str] = None,
        idle_seconds: int = DEFAULT_IDLE_SECONDS,
        warmup_seconds: int = DEFAULT_WARMUP_SECONDS,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
        seed: int = DEFAULT_SEED,
    ) -> dict:
        self._validate_phase_seconds(
            idle_seconds=idle_seconds,
            warmup_seconds=warmup_seconds,
            duration_seconds=duration_seconds,
            cooldown_seconds=cooldown_seconds,
        )

        root_metadata = doc.setdefault("metadata", {})
        root_metadata["name"] = job_name
        root_metadata["namespace"] = namespace
        root_metadata.setdefault("labels", {})["workload"] = "cpu-burn"

        template = doc.setdefault("spec", {}).setdefault("template", {})
        template_metadata = template.setdefault("metadata", {})
        template_metadata.setdefault("labels", {})["workload"] = "cpu-burn"

        pod_spec = template.setdefault("spec", {})
        pod_spec.setdefault("restartPolicy", "Never")
        if node_name is not None:
            pod_spec.setdefault("nodeSelector", {})["kubernetes.io/hostname"] = node_name

        containers = pod_spec.get("containers") or []
        if not containers:
            raise ValueError("Job template must contain at least one container")
        # The bundled CPU template has one container named 'main'. If a custom
        # template has multiple containers, prefer 'main' and fall back to the first.
        target_container = next((c for c in containers if c.get("name") == "main"), containers[0])
        self._upsert_env(target_container, "IDLE_SECONDS", str(idle_seconds))
        self._upsert_env(target_container, "WARMUP_SECONDS", str(warmup_seconds))
        self._upsert_env(target_container, "DURATION_SECONDS", str(duration_seconds))
        self._upsert_env(target_container, "COOLDOWN_SECONDS", str(cooldown_seconds))
        self._upsert_env(target_container, "WORKLOAD_SEED", str(seed))

        return doc

    def _build_unique_job_name(self, base_job_name: str, launch_index: int, timestamp: str = None) -> str:
        if timestamp is None:
            timestamp = self.now_utc_compact()
        return f"{base_job_name}-{timestamp}-{launch_index}"[:63].rstrip("-").lower()

    def launch_single_job(
        self,
        namespace: str,
        job_name: str,
        node_name: Optional[str] = None,
        idle_seconds: int = DEFAULT_IDLE_SECONDS,
        warmup_seconds: int = DEFAULT_WARMUP_SECONDS,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
        seed: int = DEFAULT_SEED,
    ) -> dict:
        job_name = self.sanitize_k8s_name(job_name)
        doc = self.load_yaml(self.template_path)
        doc = self.apply_overrides(
            doc=doc,
            namespace=namespace,
            job_name=job_name,
            node_name=node_name,
            idle_seconds=idle_seconds,
            warmup_seconds=warmup_seconds,
            duration_seconds=duration_seconds,
            cooldown_seconds=cooldown_seconds,
            seed=seed,
        )

        created = self._batch_api.create_namespaced_job(namespace=namespace, body=doc)

        payload = {
            "namespace": created.metadata.namespace,
            "job_name": created.metadata.name,
            "uid": created.metadata.uid,
            "created_at": created.metadata.creation_timestamp.isoformat() if created.metadata.creation_timestamp else None,
            "idle_seconds": idle_seconds,
            "warmup_seconds": warmup_seconds,
            "duration_seconds": duration_seconds,
            "cooldown_seconds": cooldown_seconds,
            "seed": seed,
        }
        return payload

    def launch_multiple_jobs(
        self,
        namespace: str,
        job_name: str,
        n: int,
        node_name: Optional[str] = None,
        idle_seconds: int = DEFAULT_IDLE_SECONDS,
        warmup_seconds: int = DEFAULT_WARMUP_SECONDS,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
        seed: int = DEFAULT_SEED,
    ) -> List[dict]:
        results = []
        timestamp = self.now_utc_compact()
        for i in range(n):
            unique_job_name = self._build_unique_job_name(job_name, i, timestamp)
            result = self.launch_single_job(
                namespace=namespace,
                job_name=unique_job_name,
                node_name=node_name,
                idle_seconds=idle_seconds,
                warmup_seconds=warmup_seconds,
                duration_seconds=duration_seconds,
                cooldown_seconds=cooldown_seconds,
                seed=seed,
            )
            results.append(result)
        return results

    def _get_job_terminal_state(self, job: client.V1Job) -> Tuple[Optional[str], Optional[datetime]]:
        status = job.status

        for condition in status.conditions or []:
            if condition.type == "Complete" and condition.status == "True":
                return "succeeded", condition.last_transition_time
            if condition.type == "Failed" and condition.status == "True":
                return "failed", condition.last_transition_time
        return None, None

    def delete_job(self, namespace: str, job_name: str) -> None:
        body = client.V1DeleteOptions(propagation_policy="Background")
        try:
            self._batch_api.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                body=body,
            )
            logger.debug(f"Deleted job {namespace}/{job_name} from Kubernetes.")
        except ApiException as exc:
            if exc.status == 404:
                logger.debug(f"Job {namespace}/{job_name} was already deleted.")
                return
            raise

    def _check_and_delete_jobs(
        self,
        tracked_jobs: Dict[Tuple[str, str], dict],
        remaining_jobs: Set[Tuple[str, str]],
        delete_after_seconds: int
    ) -> None:
        """Checks tracked jobs and deletes terminal jobs after the configured retention period."""
        now = self.now_utc()
        for namespace, job_name in list(remaining_jobs):
            job_info = tracked_jobs[(namespace, job_name)]
            try:
                current_job = self._batch_api.read_namespaced_job(name=job_name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    logger.debug(f"Job {namespace}/{job_name} not found (might have been deleted)")
                    remaining_jobs.remove((namespace, job_name))
                    job_info.setdefault("terminal_state", "deleted")
                    continue
                logger.error(f"Error fetching job {namespace}/{job_name}: {e}")
                raise

            # Check if the job has reached a terminal state and update tracking info
            # if we have not already recorded it.
            if job_info.get("terminal_state") is None:
                terminal_state, completed_at = self._get_job_terminal_state(current_job)
                if terminal_state:
                    delete_at = (completed_at or now) + timedelta(seconds=delete_after_seconds)
                    job_info["terminal_state"] = terminal_state
                    job_info["finished_at"] = completed_at.isoformat() if completed_at else None
                    job_info["to_be_deleted_at"] = delete_at.isoformat()
                    logger.debug(
                        f"Job {namespace}/{job_name} reached terminal state: {terminal_state}."
                        f" It will be deleted at {delete_at.isoformat()}."
                    )

            # Check if it is time to delete the job.
            to_delete_at_str = job_info.get("to_be_deleted_at")
            if to_delete_at_str:
                delete_at = datetime.fromisoformat(to_delete_at_str)
                if now >= delete_at:
                    self.delete_job(namespace, job_name)
                    job_info.setdefault("terminal_state", "deleted")
                    job_info["deleted_at"] = now.isoformat()
                    remaining_jobs.remove((namespace, job_name))

    def _wait_with_cleanup(
        self,
        tracked_jobs: Dict[Tuple[str, str], dict],
        remaining_jobs: Set[Tuple[str, str]],
        wait_seconds: float,
        delete_after_seconds: int,
        status_poll_seconds: int,
    ) -> None:
        """Waits while periodically deleting jobs that reached terminal states."""
        deadline = self.now_utc() + timedelta(seconds=wait_seconds)

        while True:
            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )

            remaining_wait_seconds = max(0.0, (deadline - self.now_utc()).total_seconds())
            if remaining_wait_seconds <= 0:
                return

            sleep_seconds = min(status_poll_seconds, remaining_wait_seconds)
            logger.info(f"Still tracking {len(remaining_jobs)} job(s). Waiting {remaining_wait_seconds:.1f} more second(s) before the next action...")
            time.sleep(sleep_seconds)

    def _track_launched_jobs(self, launched_jobs: List[dict], tracked_jobs: Dict[Tuple[str, str], dict], remaining_jobs: Set[Tuple[str, str]]) -> None:
        for job in launched_jobs:
            job_key = (job["namespace"], job["job_name"])
            tracked_jobs[job_key] = job
            remaining_jobs.add(job_key)

    def _drain_remaining_jobs(
        self,
        tracked_jobs: Dict[Tuple[str, str], dict],
        remaining_jobs: Set[Tuple[str, str]],
        total_jobs: int,
        delete_after_seconds: int,
        status_poll_seconds: int,
    ) -> List[dict]:
        logger.info(f"All {total_jobs} jobs launched. Continuing cleanup until all tracked jobs are deleted...")
        while remaining_jobs:
            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )
            if remaining_jobs:
                logger.info(f"Still tracking {len(remaining_jobs)} job(s). Polling again in {status_poll_seconds} seconds...")
                time.sleep(status_poll_seconds)
        return list(tracked_jobs.values())

    def run_batch(
        self,
        namespace: str,
        job_name: str,
        total_jobs: int,
        batch_size: int,
        wait_seconds: int,
        delete_after_seconds: int,
        status_poll_seconds: int,
        seed: int = DEFAULT_SEED,
        idle_seconds: int = DEFAULT_IDLE_SECONDS,
        warmup_seconds: int = DEFAULT_WARMUP_SECONDS,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    ) -> List[dict]:
        tracked_jobs: Dict[Tuple[str, str], dict] = {}
        remaining_jobs: Set[Tuple[str, str]] = set()
        launched_count = 0
        self._validate_phase_seconds(idle_seconds, warmup_seconds, duration_seconds, cooldown_seconds)

        while launched_count < total_jobs:
            current_batch_size = min(batch_size, total_jobs - launched_count)
            batch_jobs = self.launch_multiple_jobs(
                namespace=namespace,
                job_name=job_name,
                n=current_batch_size,
                idle_seconds=idle_seconds,
                warmup_seconds=warmup_seconds,
                duration_seconds=duration_seconds,
                cooldown_seconds=cooldown_seconds,
                seed=seed,
            )
            self._track_launched_jobs(batch_jobs, tracked_jobs, remaining_jobs)
            launched_count += current_batch_size

            # After launching each batch, check the status of all tracked jobs and
            # delete terminal jobs that are old enough before waiting for the next batch.
            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )

            if launched_count < total_jobs:
                logger.info(f"Launched {launched_count}/{total_jobs} jobs. Continuing cleanup polling while waiting {wait_seconds} seconds before launching next batch...")
                self._wait_with_cleanup(
                    tracked_jobs=tracked_jobs,
                    remaining_jobs=remaining_jobs,
                    wait_seconds=wait_seconds,
                    delete_after_seconds=delete_after_seconds,
                    status_poll_seconds=status_poll_seconds,
                )

        return self._drain_remaining_jobs(
            tracked_jobs=tracked_jobs,
            remaining_jobs=remaining_jobs,
            total_jobs=total_jobs,
            delete_after_seconds=delete_after_seconds,
            status_poll_seconds=status_poll_seconds,
        )

    def run_poisson_stages(
        self,
        namespace: str,
        job_name: str,
        workloads: List[WorkloadStage],
        delete_after_seconds: int,
        status_poll_seconds: int,
        seed: int = DEFAULT_SEED,
        idle_seconds: int = DEFAULT_IDLE_SECONDS,
        warmup_seconds: int = DEFAULT_WARMUP_SECONDS,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    ) -> List[dict]:
        tracked_jobs: Dict[Tuple[str, str], dict] = {}
        remaining_jobs: Set[Tuple[str, str]] = set()
        rng = random.Random(seed)
        total_jobs = sum(stage.amount for stage in workloads)
        total_launch_count = 0
        self._validate_phase_seconds(idle_seconds, warmup_seconds, duration_seconds, cooldown_seconds)

        for stage_idx, stage in enumerate(workloads):
            logger.info(
                f"Starting workload stage {stage_idx + 1}/{len(workloads)}: "
                f"{stage.amount} jobs with mean IAT {stage.iat_seconds:.2f} seconds"
            )
            for launch_idx in range(stage.amount):
                unique_job_name = self._build_unique_job_name(job_name, total_launch_count)
                launched_job = self.launch_single_job(
                    namespace=namespace,
                    job_name=unique_job_name,
                    idle_seconds=idle_seconds,
                    warmup_seconds=warmup_seconds,
                    duration_seconds=duration_seconds,
                    cooldown_seconds=cooldown_seconds,
                    seed=seed,
                )
                self._track_launched_jobs([launched_job], tracked_jobs, remaining_jobs)
                total_launch_count += 1

                logger.info(
                    f"Launched {launch_idx + 1}/{stage.amount} of stage {stage_idx + 1} "
                    f"({total_launch_count}/{total_jobs} total) with CPU burn "
                    f"idle={idle_seconds}s warmup={warmup_seconds}s "
                    f"duration={duration_seconds}s cooldown={cooldown_seconds}s seed={seed}"
                )

                self._check_and_delete_jobs(
                    tracked_jobs=tracked_jobs,
                    remaining_jobs=remaining_jobs,
                    delete_after_seconds=delete_after_seconds,
                )

                if launch_idx < stage.amount - 1:
                    sampled_wait_seconds = rng.expovariate(1.0 / stage.iat_seconds)
                    logger.info(
                        f"Waiting {sampled_wait_seconds:.2f} seconds before the next Poisson arrival "
                        f"(stage mean IAT={stage.iat_seconds:.2f}s)..."
                    )
                    self._wait_with_cleanup(
                        tracked_jobs=tracked_jobs,
                        remaining_jobs=remaining_jobs,
                        wait_seconds=sampled_wait_seconds,
                        delete_after_seconds=delete_after_seconds,
                        status_poll_seconds=status_poll_seconds,
                    )

        return self._drain_remaining_jobs(
            tracked_jobs,
            remaining_jobs,
            total_jobs,
            delete_after_seconds,
            status_poll_seconds,
        )

    def run_poisson(
        self,
        namespace: str,
        job_name: str,
        total_jobs: int,
        iat_seconds: float,
        delete_after_seconds: int,
        status_poll_seconds: int,
        seed: int = DEFAULT_SEED,
        idle_seconds: int = DEFAULT_IDLE_SECONDS,
        warmup_seconds: int = DEFAULT_WARMUP_SECONDS,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    ) -> List[dict]:
        workload = WorkloadStage(amount=total_jobs, iat_seconds=iat_seconds)
        return self.run_poisson_stages(
            namespace=namespace,
            job_name=job_name,
            workloads=[workload],
            delete_after_seconds=delete_after_seconds,
            status_poll_seconds=status_poll_seconds,
            seed=seed,
            idle_seconds=idle_seconds,
            warmup_seconds=warmup_seconds,
            duration_seconds=duration_seconds,
            cooldown_seconds=cooldown_seconds,
        )

    def run_poisson_config(self, generator_config: GeneratorConfig) -> List[dict]:
        return self.run_poisson_stages(
            namespace=generator_config.namespace,
            job_name=generator_config.job_name,
            workloads=generator_config.workloads,
            delete_after_seconds=generator_config.delete_after_seconds,
            status_poll_seconds=generator_config.status_poll_seconds,
            seed=generator_config.seed,
            idle_seconds=generator_config.idle_seconds,
            warmup_seconds=generator_config.warmup_seconds,
            duration_seconds=generator_config.duration_seconds,
            cooldown_seconds=generator_config.cooldown_seconds,
        )
