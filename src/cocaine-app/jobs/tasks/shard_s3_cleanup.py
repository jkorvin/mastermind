import logging

from jobs import JobBrokenError, RetryError, TaskTypes
from minion_cmd import MinionCmdTask
import storage


logger = logging.getLogger('mm.jobs')


class ShardS3CleanupTask(MinionCmdTask):
    def __init__(self, job):
        super(ShardS3CleanupTask, self).__init__(job)
        self.type = TaskTypes.TYPE_S3_SHARD_CLEANUP_TASK

    def execute(self, processor):
        # checking if task still applicable
        logger.info('Job {0}, task {1}: starting cleanup S3 database on the shard {2}'.format(
            self.parent_job.id, self.id, self.node))

        super(ShardS3CleanupTask, self).execute(processor)
