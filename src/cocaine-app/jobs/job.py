import logging
import msgpack
import os.path
import time
import uuid

from error import JobBrokenError
import helpers as h
import keys
from mastermind_core.db.mongo import MongoObject
from mastermind_core.config import config
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    InconsistentLockError,
    LockAlreadyAcquiredError,
    API_ERROR_CODE
)
from tasks import TaskFactory

import pymongo


logger = logging.getLogger('mm.jobs')

RESTORE_CFG = config.get('restore', {})


class Job(MongoObject):

    STATUS_NOT_APPROVED = 'not_approved'
    STATUS_NEW = 'new'
    STATUS_EXECUTING = 'executing'
    STATUS_PENDING = 'pending'
    STATUS_BROKEN = 'broken'
    STATUS_COMPLETED = 'completed'
    STATUS_CANCELLED = 'cancelled'

    GROUP_LOCK_PREFIX = 'group/'
    COUPLE_LOCK_PREFIX = 'couple/'

    ACTIVE_STATUSES = (
        STATUS_NOT_APPROVED,
        STATUS_NEW,
        STATUS_EXECUTING,
        STATUS_PENDING,
        STATUS_BROKEN,
    )

    RESOURCE_FS = 'fs'
    RESOURCE_HOST_IN = 'host_in'
    RESOURCE_HOST_OUT = 'host_out'
    RESOURCE_CPU = 'cpu'

    COMMON_PARAMS = ('need_approving',)

    GROUP_FILE_MARKER_PATH = RESTORE_CFG.get('group_file_marker')
    GROUP_FILE_DIR_MOVE_SRC_RENAME = RESTORE_CFG.get('group_file_dir_move_src_rename')
    BACKEND_DOWN_MARKER = RESTORE_CFG.get('backend_down_marker')
    IDS_FILE_PATH = RESTORE_CFG.get('ids_file')

    BACKEND_COMMANDS_CFG = config.get('backend_commands', {})

    DNET_CLIENT_ALREADY_IN_PROGRESS = -114

    def __init__(self, need_approving=False):
        self.id = uuid.uuid4().hex
        self.status = (self.STATUS_NOT_APPROVED
                       if need_approving else
                       self.STATUS_NEW)
        self._create_ts = None
        self._start_ts = None
        self._update_ts = None
        self._finish_ts = None
        self.type = None
        self.tasks = []
        self.error_msg = []

    @classmethod
    def new(cls, session, **kwargs):
        super(Job, cls).new(session, **kwargs)

        cparams = {}
        for cparam in cls.COMMON_PARAMS:
            if cparam in kwargs:
                cparams[cparam] = kwargs[cparam]
        job = cls(**cparams)
        for param in cls.PARAMS:
            setattr(job, param, kwargs.get(param))
        ts = time.time()
        job.create_ts = ts
        job.update_ts = ts
        job._dirty = True

        job._check_job()

        try:
            job._set_resources()
        except Exception as e:
            logger.exception('Job {}: failed to set job resources'.format(job.id))
            raise

        try:
            job.perform_locks()
        except LockFailedError as e:
            logger.error('Job {0}: failed to perform required locks: {1}'.format(job.id, e))
            raise
        except LockError:
            logger.error('Job {0}: failed to perform required locks'.format(job.id))
            raise

        try:
            job.mark_groups(session)
        except Exception as e:
            logger.error('Job {0}: failed to mark required groups: {1}'.format(job.id, e))
            job.release_locks()
            raise

        return job

    def _check_job(self):
        """Check job parameters and prerequisites.

        This method should make sure that job tasks can be created, resources list can be
        constructed, etc.
        """
        pass

    def _set_resources(self):
        raise NotImplemented('_set_resources method for {} should be '
            'implemented'.format(type(self).__name__))

    @classmethod
    def from_data(cls, data):
        job = cls()
        job.load(data)
        return job

    def load(self, data):
        self.id = data['id'].encode('utf-8')
        self.status = data['status']
        self.create_ts = data.get('create_ts') or data['start_ts']
        self.start_ts = data['start_ts']
        self.finish_ts = data['finish_ts']
        self.update_ts = data.get('update_ts') or self.finish_ts or self.start_ts
        self.type = data['type']
        self.error_msg = data.get('error_msg', [])

        self.tasks = [TaskFactory.make_task(task_data, self) for task_data in data['tasks']]

        for param in self.PARAMS:
            val = data.get(param)
            if isinstance(val, unicode):
                val = val.encode('utf-8')
            setattr(self, param, val)

        return self

    def make_path(self, path, base_path=None):
        if not path:
            return ''
        if os.path.isabs(path):
            return path
        if base_path:
            path = os.path.join(base_path, path)
        return path

    @property
    def create_ts(self):
        return self._create_ts

    @create_ts.setter
    def create_ts(self, value):
        if value is None:
            self._create_ts = None
        else:
            self._create_ts = int(value)

    @property
    def start_ts(self):
        return self._start_ts

    @start_ts.setter
    def start_ts(self, value):
        if value is None:
            self._start_ts = None
        else:
            self._start_ts = int(value)

    @property
    def update_ts(self):
        return self._update_ts

    @update_ts.setter
    def update_ts(self, value):
        if value is None:
            self._update_ts = None
        else:
            self._update_ts = int(value)

    @property
    def finish_ts(self):
        return self._finish_ts

    @finish_ts.setter
    def finish_ts(self, value):
        if value is None:
            self._finish_ts = None
        else:
            self._finish_ts = int(value)

    def _dump(self):
        data = {'id': self.id,
                'status': self.status,
                'create_ts': self.create_ts,
                'start_ts': self.start_ts,
                'update_ts': self.update_ts,
                'finish_ts': self.finish_ts,
                'type': self.type,
                'error_msg': self.error_msg}

        data.update({
            k: getattr(self, k)
            for k in self.PARAMS
        })
        return data

    def dump(self):
        data = self._dump()
        data['tasks'] = [task.dump() for task in self.tasks]
        return data

    def human_dump(self):
        data = self._dump()
        data['tasks'] = [task.human_dump() for task in self.tasks]
        return data

    def node_backend(self, host, port, backend_id, family=None):
        if not family:
            # old backend id compatibility
            return '{host}:{port}/{backend_id}'.format(
                host=host,
                port=port,
                backend_id=backend_id,
            )

        return '{host}:{port}:{family}/{backend_id}'.format(
            host=host,
            port=port,
            family=family,
            backend_id=backend_id,
        )

    def create_tasks(self, processor):
        raise RuntimeError('Job creation should be implemented '
            'in derived class')

    def perform_locks(self):
        try:
            sync_manager.persistent_locks_acquire(self._locks, self.id)
        except LockAlreadyAcquiredError as e:
            if e.holder_id != self.id:
                logger.error('Job {0}: {1} is already '
                    'being processed by job {2}'.format(self.id, e.lock_id, e.holder_id))

                last_error = self.error_msg and self.error_msg[-1] or None
                if last_error and (last_error.get('code') != API_ERROR_CODE.LOCK_ALREADY_ACQUIRED or
                                   last_error.get('holder_id') != e.holder_id):
                    self.add_error(e)

                raise
            else:
                logger.warn(
                    'Job {job_id}: locks {locks} are already acquired, skipping'.format(
                        job_id=self.id,
                        locks=e.lock_ids,
                    )
                )

    def mark_groups(self, session):
        for group_id, updated_meta in self._group_marks():
            s = session.clone()
            s.set_groups([group_id])
            packed = msgpack.packb(updated_meta)
            _, failed_group = h.write_retry(
                s, keys.SYMMETRIC_GROUPS_KEY, packed)
            if failed_group:
                logger.error('Job {0}: failed to mark group {1}'.format(
                    self.id, group_id))

    def _group_marks(self):
        """
        Optional method for subclasses to mark groups.
        Returns iterable of (group, updated_meta)
        """
        return []

    def unmark_groups(self, session):
        for group_id, updated_meta in self._group_unmarks():
            s = session.clone()
            s.set_groups([group_id])
            packed = msgpack.packb(updated_meta)
            _, failed_group = h.write_retry(
                s, keys.SYMMETRIC_GROUPS_KEY, packed)
            if failed_group:
                logger.error('Job {0}: failed to unmark group {1}'.format(
                    self.id, group_id))

    def _group_unmarks(self):
        """
        Optional method for subclasses to mark groups.
        Returns iterable of (group, updated_meta)
        """
        return []

    def release_locks(self):
        try:
            sync_manager.persistent_locks_release(self._locks, self.id)
        except InconsistentLockError as e:
            logger.error('Job {0}: some of the locks {1} are already acquired by another '
                'job {2}'.format(self.id, self._locks, e.holder_id))

    @property
    def _locks(self):
        return (['{0}{1}'.format(self.GROUP_LOCK_PREFIX, group)
                 for group in self._involved_groups] +
                ['{0}{1}'.format(self.COUPLE_LOCK_PREFIX, couple)
                 for couple in self._involved_couples])

    @property
    def involved_uncoupled_groups(self):
        '''Returns uncoupled groups' ids that are involved in the job
        '''
        return []

    @property
    def _involved_groups(self):
        raise NotImplementedError('_involved_groups property-method for {} should be '
            'implemented'.format(type(self).__name__))

    @property
    def _involved_couples(self):
        raise NotImplementedError('_involved_couples property-method for {} should be '
            'implemented'.format(type(self).__name__))

    def check_node_backends(self, group, node_backends_count=1):
        if len(group.node_backends) != node_backends_count:
            raise JobBrokenError('Group {0} cannot be used for job, '
                                 'it has {1} node backends, 1 expected'.format(
                                     group.group_id, len(group.node_backends)))

    def complete(self, processor):
        if self.status == self.STATUS_COMPLETED:
            self.on_complete(processor)
        try:
            self.unmark_groups(processor.session)
        except Exception as e:
            logger.error('Job {0}: failed to unmark required groups: {1}'.format(
                self.id, e))
            raise
        ts = time.time()
        if not self.start_ts:
            self.start_ts = ts
        self.update_ts = ts
        self.finish_ts = ts
        self.release_locks()

    def add_error(self, e):
        error_msg = e.dump()
        error_msg['ts'] = time.time()
        self.error_msg.append(error_msg)

    def add_error_msg(self, msg):
        self.error_msg.append({'ts': time.time(), 'msg': msg})

    def on_start(self):
        """
        Performs checkings before starting the job.
        Should through JobBrokenError if job should not be started.
        """
        pass

    def on_complete(self, processor):
        """
        Performs action after job completion.
        """
        pass

    @staticmethod
    def list(collection, **kwargs):
        """
        TODO: This should be moved to some kind of Mongo Session object
        """
        params = {}
        sort_by = kwargs.pop('sort_by', 'create_ts')
        sort_by_order = kwargs.pop('sort_by_order', pymongo.DESCENDING)

        for k, v in kwargs.iteritems():
            if v is None:
                continue
            params.update(condition(k, v))

        return collection.find(params).sort(sort_by, sort_by_order)


def condition(field_name, field_val):
    if isinstance(field_val, (list, tuple)):
        return {field_name: {'$in': list(field_val)}}
    else:
        return {field_name: field_val}
