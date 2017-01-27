import logging

from jobs import JobBrokenError, RetryError, TaskTypes
from minion_cmd import MinionCmdTask
import storage
from sync import sync_manager
from sync.error import LockError


logger = logging.getLogger('mm.jobs')


class NodeBackendDefragTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('node_backend', 'group')

    def __init__(self, job):
        super(NodeBackendDefragTask, self).__init__(job)
        self.type = TaskTypes.TYPE_NODE_BACKEND_DEFRAG_TASK

    def _on_exec_start(self, processor):
        task_fs = storage.node_backends[self.node_backend].fs

        def couple_is_friendly(fs, couple):
            for group in couple.groups:
                for node_backend in group.node_backends:
                    if fs == node_backend.fs:
                        return True
            return False

        def group_will_be_free(busy_fs, group):
            if busy_fs in (nb.fs for nb in group.node_backends):
                return False
            try:
                if sync_manager.persistent_locks_are_free({nb.fs.lock for nb in group.node_backends}):
                    # all backends of the group are free, therefore the group is free
                    return True
                # some backends of the group are busy, therefore the group is not free
                return False
            except LockError as e:
                logger.info("Attempt to check locks failed: {0}".format(e))
                # since we have an exception, then we are not sure that the group is free
                return False

        def interfere_with_couple(busy_fs, couple):
            for group in couple.groups:
                if group_will_be_free(busy_fs, group):
                    return False
            return True

        for couple in storage.replicas_groupsets:
            if couple_is_friendly(task_fs, couple) and interfere_with_couple(task_fs, couple):
                # TODO create some logic that prevent infinity attempt to get locks
                self.attempts -= 1
                logger.error('Job {}, task {}: ignoring current attempt: current task can broke friendly couple {}'.format(
                    self.parent_job.id,
                    self.id,
                    couple,
                ))

                raise RetryError(
                    self.attempts,
                    "Attempt to start defragmentation on the filesystem {}"
                    ", which currently is the only free filesystem in friendly couple {}".format(
                        task_fs,
                        couple,
                    )
                )

    @property
    def needed_locks(self):
        return list(set(super(NodeBackendDefragTask, self).needed_locks).union(
            {storage.node_backends[self.node_backend].fs.lock}
        ))

    def _execute(self, processor):
        # checking if task still applicable
        logger.info('Job {0}, task {1}: checking group {2} and node backend {3} '
                    'consistency'.format(
                        self.parent_job.id, self.id, self.group, self.node_backend))

        if self.group not in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(self.group))
        if self.node_backend not in storage.node_backends:
            raise JobBrokenError('Node backend {0} is not found'.format(self.node_backend))

        group = storage.groups[self.group]
        node_backend = storage.node_backends[self.node_backend]

        if group.couple is None:
            raise JobBrokenError('Task {0}: group {1} does not belong '
                                 'to any couple'.format(self, self.group))

        if group.couple.status not in storage.GOOD_STATUSES:
            raise RetryError(self.attempts, JobBrokenError('Task {}: group {} couple status is {}'.format(
                self, self.group, group.couple.status)))

        if node_backend not in group.node_backends:
            raise JobBrokenError('Task {0}: node backend {1} does not belong to '
                                 'group {2}'.format(self, self.node_backend, self.group))

        super(NodeBackendDefragTask, self)._execute(processor)
