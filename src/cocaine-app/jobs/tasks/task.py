import functools
import logging
import time
import uuid

from mastermind_core.config import config
from jobs.error import RetryError
from run_history import RunHistoryRecord
from sync import sync_manager
from sync.error import LockAlreadyAcquiredError, LockError, InconsistentLockError

logger = logging.getLogger('mm.jobs')

JOB_CONFIG = config.get('jobs', {})


class Task(object):

    STATUS_QUEUED = 'queued'
    STATUS_EXECUTING = 'executing'
    STATUS_CLEANING = 'cleaning'
    STATUS_FAILED = 'failed'
    STATUS_SKIPPED = 'skipped'
    STATUS_COMPLETED = 'completed'
    ALL_STATUSES = (STATUS_QUEUED, STATUS_EXECUTING, STATUS_FAILED, STATUS_SKIPPED,
                    STATUS_COMPLETED, STATUS_CLEANING)

    PREVIOUS_STATUSES = {
        STATUS_QUEUED: (STATUS_FAILED,),
        STATUS_EXECUTING: (STATUS_QUEUED,),
        STATUS_COMPLETED: (STATUS_CLEANING,),
        STATUS_FAILED: (STATUS_CLEANING,),
        STATUS_CLEANING: (STATUS_EXECUTING, STATUS_CLEANING),
        STATUS_SKIPPED: (STATUS_FAILED,),
    }
    FINISHED_STATUSES = (STATUS_SKIPPED, STATUS_COMPLETED)

    CLEANING_LOCKS = "acquire_persistent_locks"
    CLEANING_PREPARATION = "preparation"
    CLEANING_STATUS = "status_after_clean_step"
    CLEANING_STATUSES = (STATUS_QUEUED, STATUS_FAILED, STATUS_COMPLETED)

    def __init__(self, job):
        self._status = Task.STATUS_QUEUED
        self.id = uuid.uuid4().hex
        self.type = None
        self.start_ts = None
        self.finish_ts = None
        self.attempts = 0
        self.error_msg = []
        self.run_history = []
        self.parent_job = job
        self.cleaning_details = {}

    @staticmethod
    def set_status_to_cleaning_on_error(error_description):
        def wrapper(function):
            @functools.wraps(function)
            def wrapped_function(self, *args, **kwargs):
                try:
                    return function(self, *args, **kwargs)
                except RetryError as e:
                    logger.error('Job {}, task {}: retry error: {}'.format(self.parent_job.id, self.id, e))
                    self.set_status(Task.STATUS_CLEANING)
                    raise
                except Exception as e:
                    logger.exception('Job {}, task {}: {}'.format(
                        self.parent_job.id,
                        self.id,
                        error_description,
                    ))
                    self.set_status(Task.STATUS_CLEANING)
                    raise
            return wrapped_function
        return wrapper

    def _on_exec_start(self, processor):
        """
        Called every time task changes status from 'queued' to 'executing'
        Should not be called directly, use _wrapped_on_exec_start(processor) instead
        """
        pass

    @set_status_to_cleaning_on_error.__func__("failed to execute task start handler")
    def _wrapped_on_exec_start(self, processor):
        self.cleaning_details[Task.CLEANING_PREPARATION] = self._on_exec_start(processor)
        logger.info('Job {}, task {}: preparation completed'.format(self.parent_job.id, self.id))

    # self.status is an indicator either task is successfully finished or not
    def _on_exec_stop(self, processor):
        """
        Called every time task changes status from 'executing' to anything else
        Should not be called directly, use _wrapped_on_exec_stop(processor) instead
        """
        pass

    @set_status_to_cleaning_on_error.__func__("failed to execute task stop handler")
    def _wrapped_on_exec_stop(self, processor):
        if Task.CLEANING_PREPARATION in self.cleaning_details:
            self._on_exec_stop(processor)
            logger.info('Job {}, task {}: postprocessing completed'.format(self.parent_job.id, self.id))
            del self.cleaning_details[Task.CLEANING_PREPARATION]

    def _execute(self, processor):
        """
        Should not be called directly, use _start_executing(self, processor) instead
        If an exception is occurred, you can raise RetryError in attempt to retry the task,
        if current state of the task is the same as before this ._execute(_) was called.
        """
        raise NotImplementedError("Children class should override this function")

    def _terminate(self, processor):
        """
        Should not be called directly, use stop(self, processor) instead.

        If you want to implement this function as:
            def _terminate(self, processor):
                pass
        Then make sure that cleanup phase, which will be after ._terminate(_, _) successfully finished
        do not break logic of the current execution (you can always raise an exception to prevent this).
        """
        raise NotImplementedError("Children class should override this function")

    @set_status_to_cleaning_on_error.__func__("failed to stop task")
    def stop(self, processor):
        # after task is executed, it can leave some data in last_run_history_record,
        # which lead to self.next_retry_ts != None
        # to prevent this we need to clear last_run_history_record
        self._add_history_record()
        self.on_run_history_update(error="Task is stopped")

        if self.status == Task.STATUS_EXECUTING:
            # if termination is raise exception, then we can't to call cleaning safe
            # (probably we can, but initially we don't do this)
            self._terminate(processor)

            self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED
            self.set_status(Task.STATUS_CLEANING)

            self.clean(processor)
        else:
            assert self.status == Task.STATUS_CLEANING
            self.set_status(Task.STATUS_FAILED)

        # if RetryError is raised during cleanup phase, then self.status == STATUS_CLEANING
        if self.status != self.STATUS_FAILED:
            raise RuntimeError("We terminated task, but failed to cleanup after")

    def _update_status(self, processor):
        raise NotImplementedError("Children class should override this function")

    @set_status_to_cleaning_on_error.__func__("failed to update status")
    def update_status(self, processor):
        return self._update_status(processor)

    def _finished(self, processor):
        raise NotImplementedError("Children class should override this function")

    @set_status_to_cleaning_on_error.__func__("failed to check status is finished")
    def finished(self, processor):
        return self._finished(processor)

    def _failed(self, processor):
        raise NotImplementedError("Children class should override this function")

    @set_status_to_cleaning_on_error.__func__("failed to check status is failed")
    def failed(self, processor):
        return self._failed(processor)

    @set_status_to_cleaning_on_error.__func__("failed to start execution")
    def _start_executing(self, processor):
        self.start_ts, self.finish_ts = time.time(), None
        self._execute(processor)
        logger.info('Job {}, task {}: execution successfully started'.format(
            self.parent_job.id,
            self.id
        ))

    @property
    def needed_locks(self):
        return []

    @set_status_to_cleaning_on_error.__func__("failed to acquire persistent locks")
    def _acquire_locks(self):
        logger.info('Job {}, task {}: try to acquire persistent locks {}, with data {}'.format(
            self.parent_job.id,
            self.id,
            self.needed_locks,
            self.human_id,
        ))

        try:
            # at first we try to acquire all needed locks, even if part of them is already acquired
            sync_manager.persistent_locks_acquire(self.needed_locks, data=self.human_id)

        except LockAlreadyAcquiredError as e:
            # it is acceptable only if locks are already acquired by us
            if e.holder_id != self.human_id:
                # TODO create some logic that prevent infinity attempt to get locks
                self.attempts -= 1
                logger.error('Job {}, task {}: ignoring current attempt: we cannot acquire locks {}'.format(
                    self.parent_job.id,
                    self.id,
                    self.needed_locks,
                ))

                raise RetryError(self.attempts, e)

            # logically it is impossible, but if somehow some locks are already acquire by us,
            # then we can proceed after we acquire the rest of locks.
            remained_locks = set(self.needed_locks) - set(e.lock_ids)
            try:
                sync_manager.persistent_locks_acquire(list(remained_locks), data=self.human_id)
            except LockError as e1:
                raise RetryError(self.attempts, e1)

        except LockError as e:
            raise RetryError(self.attempts, e)

        self.cleaning_details[Task.CLEANING_LOCKS] = self.needed_locks

        logger.info('Job {}, task {}: persistent locks {} are acquired with data {}'.format(
            self.parent_job.id,
            self.id,
            self.needed_locks,
            self.human_id,
        ))

    @set_status_to_cleaning_on_error.__func__("failed to release persistent locks")
    def _free_locks(self):
        if Task.CLEANING_LOCKS not in self.cleaning_details:
            return

        lock_to_release = self.cleaning_details[Task.CLEANING_LOCKS]
        logger.info('Job {}, task {}: try to release persistent locks {}'.format(
            self.parent_job.id,
            self.id,
            lock_to_release,
        ))
        try:
            sync_manager.persistent_locks_release(lock_to_release, check=self.human_id)
        except InconsistentLockError as e:
            logger.error(
                'Job {}, task {}: some of the locks {} are already acquired by someone else'.format(
                    self.parent_job.id,
                    self.id,
                    self.needed_locks,
                    e.holder_id
                )
            )
            # InconsistentLockError means, locks are free from us
            # (also, that someone is already acquired one of them)
            pass
        except LockError as e:
            raise RetryError(self.attempts, e)

        del self.cleaning_details[Task.CLEANING_LOCKS]

        logger.info('Job {}, task {}: persistent locks {} released'.format(
            self.parent_job.id,
            self.id,
            lock_to_release,
        ))

    @classmethod
    def new(cls, job, **kwargs):
        task = cls(job)
        for param in cls.PARAMS:
            setattr(task, param, kwargs.get(param))
        return task

    @classmethod
    def from_data(cls, data, job):
        task = cls(job)
        task.load(data)
        return task

    def load(self, data):
        # TODO: remove 'or' part
        self.id = data['id'] or uuid.uuid4().hex
        self._status = data['status']
        self.type = data['type']
        self.start_ts = data['start_ts']
        self.finish_ts = data['finish_ts']
        self.error_msg = data['error_msg']
        self.attempts = data.get('attempts', 0)
        self.cleaning_details = data.get('cleaning_details', {})

        self.run_history = [
            RunHistoryRecord(run_history_record_data)
            for run_history_record_data in data.get('run_history', [])
        ]

        for param in self.PARAMS:
            val = data.get(param)
            if isinstance(val, unicode):
                val = val.encode('utf-8')
            setattr(self, param, val)

    def dump(self):
        res = {
            'status': self._status,
            'id': self.id,
            'type': self.type,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'error_msg': self.error_msg,
            'attempts': self.attempts,
            'cleaning_details': self.cleaning_details,
            'run_history': [
                run_history_record.dump()
                for run_history_record in self.run_history
            ],
        }
        res.update({
            k: getattr(self, k)
            for k in self.PARAMS
        })
        return res

    def human_dump(self):
        return self.dump()

    def __str__(self):
        raise RuntimeError('__str__ method should be implemented in derived class')

    def _make_new_history_record(self):
        return RunHistoryRecord({
            RunHistoryRecord.START_TS: None,
            RunHistoryRecord.FINISH_TS: None,
            RunHistoryRecord.STATUS: None,
            RunHistoryRecord.ARTIFACTS: {},
            RunHistoryRecord.ERROR_MSG: None,
            RunHistoryRecord.DELAYED_TILL_TS: None,
        })

    def _add_history_record(self):
        record = self._make_new_history_record()
        record.start_ts = int(time.time())
        self.run_history.append(record)
        return record

    @property
    def last_run_history_record(self):
        return self.run_history[-1]

    def on_run_history_update(self, error=None):
        if not self.run_history:
            return
        last_record = self.last_run_history_record
        last_record.finish_ts = int(time.time())
        if self._status == Task.STATUS_FAILED or error:
            last_record.status = 'error'
            if error:
                last_record.error_msg = str(error)
            last_record.delayed_till_ts = self.next_retry_ts
        else:
            last_record.status = 'success'
            last_record.delayed_till_ts = None
            last_record.error_msg = None

    def ready_for_retry(self, processor):
        if not self.run_history:
            return False
        last_record = self.last_run_history_record
        if last_record.delayed_till_ts and time.time() > last_record.delayed_till_ts:
            return True
        return False

    @property
    def next_retry_ts(self):
        """ Timestamp of the next attempt of task retry after an error.

        Task types that are subject to automatic retries should implement
        this property.

        'None' is interpreted as no automatic retry attempts.
        """
        return None

    @property
    def status(self):
        return self._status

    def set_status(self, status):
        if status not in Task.ALL_STATUSES:
            raise ValueError(
                "Attempt to change task status, unknown value: {}. Accepted statuses: {}".format(
                    status,
                    Task.ALL_STATUSES,
                )
            )

        if self._status not in Task.PREVIOUS_STATUSES[status]:
            raise ValueError(
                'Job {job_id}, task {task_id}: attempt to change task status to {new}, '
                'current status is {current}, but expected one of {expected}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    new=status,
                    current=self.status,
                    expected=Task.PREVIOUS_STATUSES[status],
                )
            )

        self._status = status

        if status == Task.STATUS_CLEANING:
            return

        if status in (Task.STATUS_FAILED, Task.STATUS_COMPLETED):
            # this attempt ended, so expected status is no more necessary
            del self.cleaning_details[Task.CLEANING_STATUS]
            return

        if status == Task.STATUS_EXECUTING:
            # by default we expect, that status of current attempt will be STATUS_COMPLETED
            self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_COMPLETED
            self._add_history_record()
            return

        if status == Task.STATUS_QUEUED:
            return

    @staticmethod
    def clean_after_error(allow_retry=False):
        def wrapper(function):
            @functools.wraps(function)
            def wrapped_function(self, processor):
                try:
                    return function(self, processor)
                except RetryError as e:
                    self.on_run_history_update(error=e)
                    if allow_retry and self.attempts < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3):
                        self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_QUEUED
                        self.clean(processor)
                        return
                    self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED
                    self.clean(processor)
                    raise
                except Exception as e:
                    self.on_run_history_update(error=e)
                    self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED
                    self.clean(processor)
                    raise

            return wrapped_function
        return wrapper

    @clean_after_error.__func__(allow_retry=True)
    def start(self, processor):
        logger.info('Job {}, executing new task {}'.format(self.parent_job.id, self))

        self.set_status(Task.STATUS_EXECUTING)
        self.attempts += 1

        self._acquire_locks()
        self._wrapped_on_exec_start(processor)
        self._start_executing(processor)

    @clean_after_error.__func__(allow_retry=True)
    def update(self, processor):
        assert self.status == Task.STATUS_EXECUTING

        logger.info('Job {}, task {}: status update'.format(self.parent_job.id, self.id))

        self.update_status(processor)
        if not self.finished(processor):
            logger.debug('Job {}, task {}: is not finished'.format(self.parent_job.id, self.id))
            return

        if self.failed(processor):
            self.set_status(Task.STATUS_CLEANING)
            self.on_run_history_update(error="Task is failed")
            self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED
        else:
            self.set_status(Task.STATUS_CLEANING)
            # when status was changed to STATUS_EXECUTING, self.cleaning_details[Task.CLEANING_STATUS]
            # was already set to Task.STATUS_COMPLETED

        logger.debug('Job {}, task {}: is finished, status {}'.format(self.parent_job.id, self.id, self.status))

    def clean(self, processor):
        logger.info('Job {}, task {}: start cleaning'.format(self.parent_job.id, self.id))
        assert self.status == Task.STATUS_CLEANING

        clean_functions = (
            functools.partial(Task._wrapped_on_exec_stop, self, processor),
            functools.partial(Task._free_locks, self),
        )
        for function in clean_functions:
            try:
                function()
            except RetryError as e:
                if self.attempts < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3):
                    self.attempts += 1
                    logger.debug('Job {}, task {}: cleanup will continue at the next attempt'.format(
                        self.parent_job.id,
                        self.id,
                    ))
                    return
                # after task is executed, it can leave some data in last_run_history_record,
                # which lead to self.next_retry_ts != None
                # to prevent this we need to clear last_run_history_record
                self._add_history_record()
                self.on_run_history_update(error=e)
                self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED
                pass
            except Exception as e:
                # after task is executed, it can leave some data in last_run_history_record,
                # which lead to self.next_retry_ts != None
                # to prevent this we need to clear last_run_history_record
                self._add_history_record()
                self.on_run_history_update(error=e)
                self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED
                pass

        if self.cleaning_details[Task.CLEANING_STATUS] not in Task.CLEANING_STATUSES:
            raise RuntimeError("Unexpected status after cleaning {}, expected one of {}".format(
                self.cleaning_details[Task.CLEANING_STATUS],
                Task.CLEANING_STATUSES,
            ))

        if self.cleaning_details[Task.CLEANING_STATUS] == Task.STATUS_QUEUED:
            self.set_status(Task.STATUS_FAILED)
            self.set_status(Task.STATUS_QUEUED)
        else:
            self.set_status(self.cleaning_details[Task.CLEANING_STATUS])

        # if task is successfully finished, then self.status == Task.STATUS_COMPLETED
        # and self.last_run_history_record.error_msg == None;
        # if error was stored in last_run_history_record, then task is not finished
        if self.status == Task.STATUS_COMPLETED:
            self.on_run_history_update()

        logger.info('Job {}, task {}: cleaning successfully finished, following task status {}'.format(
            self.parent_job.id,
            self.id,
            self.status,
        ))

    def proceed(self, processor):
        if self.status == Task.STATUS_QUEUED:
            self.start(processor)

        if self.status == Task.STATUS_EXECUTING:
            self.update(processor)

        if self.status == Task.STATUS_CLEANING:
            self.clean(processor)

    @property
    def human_id(self):
        return 'Job {}, task {}'.format(self.parent_job.id, self.id)
