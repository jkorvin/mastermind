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

    CLEANING_STATUS = "status_after_clean_step"
    UNCLEANED_DETAILS = "uncleaned_details"
    CLEANING_STATUSES = (STATUS_QUEUED, STATUS_FAILED, STATUS_COMPLETED)

    # names for possible TaskHook:
    PREPARATION = "preparation"
    LOCKS = "acquire_persistent_locks"

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
        self.execute_was_successful = None

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
                    logger.exception('Job {}, task {}: {}: {}'.format(
                        self.parent_job.id,
                        self.id,
                        error_description,
                        e,
                    ))
                    self.set_status(Task.STATUS_CLEANING)
                    raise
            return wrapped_function
        return wrapper

    class TaskHook(object):
        def __init__(self, name, init_function, clean_function):
            self.name = name
            self.init_function = Task.set_status_to_cleaning_on_error(
                "failed to execute TaskHook {} init function".format(self.name)
            )(init_function)
            self.clean_function = Task.set_status_to_cleaning_on_error(
                "failed to execute TaskHook {} clean function".format(self.name)
            )(clean_function)

    def _task_hooks(self):
        """ If you need to do something before or after funtcion ._execute(_), then you should implement
        this function in your subclass of task. Expected to return a list of TaskHooks.
        :return:
            List of TaskHooks, each TaskHook is created by triple (hook_name, init_function, clean_function),
            where hook_name will be used to store info about TaskHook at self.cleaning_details[hook_name],
            init_function will be called with arguments (self, processor) before function ._execute(_)
            (result of init_function will be stored at self.cleaning_details[hook_name]),
            and clean_function will be called after function ._execute(_) with arguments
            (self, processor, self.cleaning_details[hook_name])

            You can extend these _task_hooks in subclass, by implementation _task_hooks in subclass.
            If you want to store some important information, remember that the result of init_function
            will be saved in self.cleaning_details[hook_name] and self.cleaning_details[hook_name] is stored in MongoDB.

        If some exception occurred, you can raise RetryError (inside any function: TaskHook's init_functions,
        self._start_executing, self._update_status, TaskHook's clean_function's) if you are sure that it is possible
        to retry the whole task in the next attempt to proceed the task. If attempt index is not too big
        (probably < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3), see code), then Task will be retried.
        But regardless of error type, corresponding clean_functions will be called for each successfully finished
        init_function in natural order before the current attempt of the task will be finished.

        More precisely:
            You are allowed to use RetryError in init_function if the state of the task after that error
            is the same as the state before init_function is called.
            Otherwise it is NOT recommended.

            You are allowed to use RetryError in clean_function in the case if successful call
            of the clean_function will lead to desired state after clean_function (i.e. clean_function is idempotent).
            Otherwise it is NOT recommended.

        If you want to implement a TaskHook, which is rarely retriable,
        try to separate it into set of retriable TaskHooks.

        Example of order of calling the TaskHook's functions and their statuses while errors occurred:
            init_function_1 - successful
            init_function_2 - successful
            init_function_3 - successful
            init_function_4 - any Error
            init_function_5 - call is skipped, since error occurred
            # _start_executing, update_status, ... - call is skipped, since error occurred
            clean_function_3 - RetryError
            clean_function_3 - ok
            clean_function_2 - Error
            clean_function_2 - following call is skipped, since last error was not some RetryError
            clean_function_1 - ok

        resulted status of Task:
            if errors are not occurred - STATUS_COMPLETED;
            if only RetryError are occurred - STATUS_QUEUED;
            if somewhere Error, which is not RetryError will occurred - STATUS_FAILED
        """
        return [
            # this is a list of currently implemented TaskHook's, which you can use
            # 1) need to implement .needed_locks(processor) to use:
            #    Task.TaskHook(Task.LOCKS, Task._acquire_locks, Task._release_locks),
            # 2) need to implement ._on_exec_start(processor) and ._on_exec_stop(processor, stored_data):
            #    Task.TaskHook(Task.PREPARATION, self._on_exec_start, self._on_exec_stop),
        ]

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

    def _acquire_locks(self, processor):
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

        logger.info('Job {}, task {}: persistent locks {} are acquired with data {}'.format(
            self.parent_job.id,
            self.id,
            self.needed_locks,
            self.human_id,
        ))
        return self.needed_locks

    def _release_locks(self, processor, locks_to_release):
        logger.info('Job {}, task {}: try to release persistent locks {}'.format(
            self.parent_job.id,
            self.id,
            locks_to_release,
        ))
        try:
            sync_manager.persistent_locks_release(locks_to_release, check=self.human_id)
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

        logger.info('Job {}, task {}: persistent locks {} released'.format(
            self.parent_job.id,
            self.id,
            locks_to_release,
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
        self.execute_was_successful = data.get('execute_was_successful', None)

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
            'execute_was_successful': self.execute_was_successful,
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
            if not self.cleaning_details.get(Task.UNCLEANED_DETAILS, {}):
                # UNCLEANED_DETAILS - is empty, therefore nothing to clean, so just start!
                if not self.execute_was_successful:
                    self.execute_was_successful = False  # None -> False or False -> False
                    return
                else:
                    logger.error(
                        'Job {}, task {}: CRITICAL internal logic error: we somehow restarted task,'
                        'which finished its work flawlessly'.format(
                            self.parent_job.id,
                            self.id,
                        )
                    )
                    # make clean to be safe?
                    self.set_status(Task.STATUS_CLEANING)
                    return
            else:
                # we need to finish, previously unfinished clean_function of TaskHook's.
                # And they stored at self.cleaning_details[Task.UNCLEANED_DETAILS]
                self.cleaning_details.update(self.cleaning_details.get(Task.UNCLEANED_DETAILS, {}))
                del self.cleaning_details[Task.UNCLEANED_DETAILS]

                # infinity loop is impossible
                self.set_status(Task.STATUS_CLEANING)
                self.on_run_history_update(error="Previous series of attempts failed to cleanup")

                # after we finish cleaning, we will start task again if there is no successful execution
                if not self.execute_was_successful:
                    self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_QUEUED
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

        for task_hook in self._task_hooks():
            assert task_hook.name not in self.cleaning_details
            self.cleaning_details[task_hook.name] = task_hook.init_function(self, processor)
            logger.info('Job {}, task {}: TaskHook {} init is completed'.format(
                self.parent_job.id, self.id, task_hook.name
            ))

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
            self.execute_was_successful = True
            self.set_status(Task.STATUS_CLEANING)
            # when status was changed to STATUS_EXECUTING, self.cleaning_details[Task.CLEANING_STATUS]
            # was already set to Task.STATUS_COMPLETED

        logger.debug('Job {}, task {}: is finished, status {}'.format(self.parent_job.id, self.id, self.status))

    def store_task_hook_data_to_clean(self, hook_name, e):
        """ Store nonretriable TaskHook data
        :param hook_name: A string, name of failed TaskHook.
        :param e: An exception or a string, which described the reason.
        :return: None
        """
        # clean function of some TaskHook is failed, therefore Task is failed ()
        # after task is executed, it can leave some data in last_run_history_record,
        # which lead to self.next_retry_ts != None
        # to prevent this we need to clear last_run_history_record
        self._add_history_record()
        self.on_run_history_update(error=e)
        self.cleaning_details[Task.CLEANING_STATUS] = Task.STATUS_FAILED

        logger.error(
            'Job {}, task {}: unretriable error during execution TaskHook {} clean function, '
            'uncleaned data {}'.format(self.parent_job.id, self.id, hook_name, self.cleaning_details[hook_name])
        )
        self.cleaning_details.setdefault(Task.UNCLEANED_DETAILS, {})[hook_name] = self.cleaning_details[hook_name]
        del self.cleaning_details[hook_name]

    def clean(self, processor):
        logger.info('Job {}, task {}: start cleaning'.format(self.parent_job.id, self.id))
        assert self.status == Task.STATUS_CLEANING

        for task_hook in self._task_hooks():
            if task_hook.name not in self.cleaning_details:
                # for this step either start function was not successfully finished or
                # clean_function is already finished successfully in previous attempts
                continue

            try:
                task_hook.clean_function(self, processor, self.cleaning_details[task_hook.name])
            except RetryError as e:
                if self.attempts < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3):
                    self.attempts += 1
                    logger.debug('Job {}, task {}: cleanup will continue at the next attempt'.format(
                        self.parent_job.id,
                        self.id,
                    ))
                    return
                self.store_task_hook_data_to_clean(task_hook.name, e)
                pass
            except Exception as e:
                self.store_task_hook_data_to_clean(task_hook.name, e)
                pass
            else:
                # clean_function is finished successfully
                del self.cleaning_details[task_hook.name]
                logger.info('Job {}, task {}: TaskHook {}, clean is completed'.format(self.parent_job.id, self.id, task_hook.name))

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
