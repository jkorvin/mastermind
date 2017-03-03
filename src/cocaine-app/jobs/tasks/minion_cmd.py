import logging
import time

from tornado.httpclient import HTTPError

from infrastructure_cache import cache
from jobs import TaskTypes, RetryError
from task import Task


logger = logging.getLogger('mm.jobs')


class MinionCmdTask(Task):

    PARAMS = ('group', 'host', 'cmd', 'params', 'minion_cmd_id')
    TASK_TIMEOUT = 6000

    MINION_RESTART_EXIT_CODE = 666

    def __init__(self, job):
        super(MinionCmdTask, self).__init__(job)
        self.minion_cmd = None
        self.minion_cmd_id = None
        self.type = TaskTypes.TYPE_MINION_CMD

    @classmethod
    def new(cls, job, **kwargs):
        task = super(MinionCmdTask, cls).new(job, **kwargs)
        if task.params is None:
            task.params = {}
        task.params['task_id'] = task.id
        return task

    def _update_status(self, processor):
        try:
            self.minion_cmd = processor.minions_monitor._get_command(self.minion_cmd_id)
            logger.debug('Job {0}, task {1}, minion command status was updated: {2}'.format(
                self.parent_job.id, self.id, self.minion_cmd))
        except ValueError:
            logger.exception(
                'Job {job_id}, task {task_id}, failed to fetch minion command "{cmd_id}" '
                'status'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    cmd_id=self.minion_cmd_id,
                )
            )
            return
        self._set_run_history_parameters(self.minion_cmd)

    def _execute(self, processor):
        try:
            minion_response = processor.minions_monitor.execute(
                self.host,
                self.cmd,
                self.params
            )
        except HTTPError as e:
            raise RetryError(self.attempts, e)
        cmd_response = minion_response.values()[0]
        self._set_minion_task_parameters(cmd_response)

    def _set_minion_task_parameters(self, minion_cmd):
        self.minion_cmd = minion_cmd
        self.minion_cmd_id = self.minion_cmd['uid']
        logger.info(
            'Job {job_id}, task {task_id}, minions task '
            'execution: {command}'.format(
                job_id=self.parent_job.id,
                task_id=self.id,
                command=self.minion_cmd
            )
        )

    def _set_run_history_parameters(self, minion_cmd):
        if not self.run_history:
            return
        record = self.last_run_history_record
        record.command_uid = self.minion_cmd['uid']
        record.exit_code = self.minion_cmd['exit_code']
        record.artifacts = self.minion_cmd.get('artifacts') or {}

    def human_dump(self):
        data = super(MinionCmdTask, self).human_dump()
        data['hostname'] = cache.get_hostname_by_addr(data['host'], strict=False)
        return data

    def finished(self, processor):
        return ((self.minion_cmd is None and
                 time.time() - self.start_ts > self.TASK_TIMEOUT) or
                (self.minion_cmd and self.minion_cmd['progress'] == 1.0))

    def failed(self, processor):
        if self.minion_cmd is None:
            return True
        return (self.minion_cmd['exit_code'] != 0 and
                self.minion_cmd.get('command_code') not in
                    self.params.get('success_codes', []))

    def __str__(self):
        return 'MinionCmdTask[id: {0}]<{1}>'.format(self.id, self.cmd)

    def _make_new_history_record(self):
        record = super(MinionCmdTask, self)._make_new_history_record()
        record.command_uid = None
        record.exit_code = None
        return record

    @property
    def next_retry_ts(self):
        last_record = self.last_run_history_record
        if last_record.status != 'error':
            return None

        if last_record.exit_code == self.MINION_RESTART_EXIT_CODE:
            return int(time.time())

        return super(MinionCmdTask, self).next_retry_ts
