import time
from copy import deepcopy
from functools import partial
from typing import Callable

import ray

from datatrove.executor.base import PipelineExecutor
from datatrove.io import DataFolderLike
from datatrove.pipeline.base import PipelineStep
from datatrove.utils.logging import logger
from datatrove.utils.stats import PipelineStats


class LocalPipelineExecutor(PipelineExecutor):
    """Executor to run a pipeline locally using Ray"""

    def __init__(
        self,
        pipeline: list[PipelineStep | Callable],
        tasks: int = 1,
        workers: int = -1,
        logging_dir: DataFolderLike = None,
        depends: "LocalPipelineExecutor" = None,
        skip_completed: bool = True,
        local_tasks: int = -1,
        local_rank_offset: int = 0,
        randomize_start_duration: int = 0,
    ):
        super().__init__(pipeline, logging_dir, skip_completed, randomize_start_duration)
        self.tasks = tasks
        self.workers = workers if workers != -1 else tasks
        self.local_tasks = local_tasks if local_tasks != -1 else tasks
        self.local_rank_offset = local_rank_offset
        self.depends = depends
        if self.local_rank_offset + self.local_tasks > self.tasks:
            raise ValueError(
                f"Local tasks go beyond the total tasks (local_rank_offset + local_tasks = {self.local_rank_offset + self.local_tasks} > {self.tasks} = tasks)"
            )
        self._launched = False

        # Initialize Ray
        ray.init(ignore_reinit_error=True, num_cpus=self.workers)

    def run(self):
        """
            Run the pipeline tasks using Ray for parallelism.
        """
        if self.depends:
            if not self.depends._launched:
                logger.info(f'Launching dependency job "{self.depends}"')
                self.depends.run()
            while (incomplete := len(self.depends.get_incomplete_ranks())) > 0:
                logger.info(f"Dependency job still has {incomplete}/{self.depends.world_size} tasks. Waiting...")
                time.sleep(2 * 60)

        self._launched = True
        if all(map(self.is_rank_completed, range(self.local_rank_offset, self.local_rank_offset + self.local_tasks))):
            logger.info(f"Not doing anything as all {self.local_tasks} tasks have already been completed.")
            return

        self.save_executor_as_json()
        
        ranks_to_run = self.get_incomplete_ranks(
            range(self.local_rank_offset, self.local_rank_offset + self.local_tasks)
        )
        if (skipped := self.local_tasks - len(ranks_to_run)) > 0:
            logger.info(f"Skipping {skipped} already completed tasks")

        if self.workers == 1:
            pipeline = self.pipeline
            stats = []
            for rank in ranks_to_run:
                self.pipeline = deepcopy(pipeline)
                stats.append(self._run_for_rank(rank, 0))
        else:
            @ray.remote(max_retries=-1)#will keep trying when failed, wait 30 mins to prevent ddos.
            def _run_for_rank_ray(rank: int, local_rank: int):
                try:
                    return self._run_for_rank(rank, local_rank)  
                except Exception as e:
                    print(f"Task failed with error: {e}. wait 30mins before proceeding.")
                    time.sleep(1800)
                    raise e

            actors = [_run_for_rank_ray for _ in range(self.workers)]
            actor_pool = ray.util.ActorPool(actors)
            for i, rank in enumerate(ranks_to_run):
                actor_pool.submit(lambda agent, v: agent.remote(v, local_rank=0), i)
            stats =[]
            while actor_pool.has_next():
                stats.append(actor_pool.get_next(timeout=None))
        # merged stats
        stats = sum(stats, start=PipelineStats())
        with self.logging_dir.open("stats.json", "wt") as statsfile:
            stats.save_to_disk(statsfile)
        logger.success(stats.get_repr(f"All {self.local_tasks} tasks"))
        return stats

    @property
    def world_size(self) -> int:
        return self.tasks