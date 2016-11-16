import logging

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import Task, NodeBackendDefragTask, CoupleDefragStateCheckTask
import storage


logger = logging.getLogger('mm.jobs')


class S3CleanupJob(Job):

    # list(db_host:db_port)
    PARAMS = ('node', # where to run
              'db_host', # parameters to script
              'db_port',
              'db_name',
              'db_user',
              'db_password',
              'db_wait',
              'db_attempts',
              'limit',
              'mds_namespace',
              'mds_auth_token',
              'mds_port',
              'mds_timeout',
              'mds_attempts',
              'dry_run',
              'db_dry_run',)

    def __init__(self, **kwargs):
        super(S3CleanupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_COUPLE_DEFRAG_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(S3CleanupJob, cls).new(*args, **kwargs)
        return job

    def _set_resources(self):
        self.resources = {
            Job.RESOURCE_CPU: [self.node.host.addr],
            Job.RESOURCE_HOST_IN: [self.node.host.addr],
            Job.RESOURCE_HOST_OUT: [self.node.host.addr],
        }

    def create_tasks(self, processor):
        # TODO some checkds

        cmd = infrastructure._shard_s3_cleanup_cmd(
            self.node.host.addr, self.node.port, self.node.family,
            # TODO ADD OTHER ARGUMENTS
        )
        task = NodeBackendDefragTask.new(
            self,
            host=self.node.host.addr,
            cmd=cmd,
            params={
                'db_host': self.db_host,
                # TODO other params
            }
        )

        self.tasks.append(task)

    @property
    def _involved_groups(self):
        return []

    @property
    def _involved_couples(self):
        return []
