import logging

from jobs import TaskTypes, JobBrokenError
from external_storage import ExternalStorageTask
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class ExternalStorageDataSizeTask(ExternalStorageTask):

    PARAMS = ExternalStorageTask.PARAMS + ('groupset_type', 'mandatory_dcs')

    def __init__(self, job):
        super(ExternalStorageDataSizeTask, self).__init__(job)
        self.type = TaskTypes.TYPE_EXTERNAL_STORAGE_DATA_SIZE

    def _task_hooks(self):
        task_hooks = super(ExternalStorageDataSizeTask, self)._task_hooks()

        def do_nothing(*args, **kwargs):
            pass

        task_hooks.append(
            Task.TaskHook(Task.PREPARATION, do_nothing, ExternalStorageDataSizeTask._on_exec_stop),
        )
        return task_hooks

    def _on_exec_stop(self, processor, stored_data):
        if self.execute_was_successful:

            command_state = processor.minions_monitor.get_minion_cmd_state(self.minion_cmd)

            try:
                data_size = self._data_size(command_state['output'])
            except ValueError as e:
                raise JobBrokenError(str(e))

            if data_size == 0:
                logger.info(
                    'Job {job_id}, task {task_id}: determined data size is 0, converting is not '
                    'required'.format(
                        job_id=self.parent_job.id,
                        task_id=self.id,
                    )
                )
                return

            total_space = 0
            groupsets = []
            selected_groups = set()

            while total_space < data_size:
                try:
                    groups = processor._select_groups_for_groupset(
                        type=self.groupset_type,
                        mandatory_dcs=self.mandatory_dcs,
                        skip_groups=selected_groups,
                    )
                except Exception as e:
                    raise JobBrokenError(str(e))
                groupsets.append(groups)
                selected_groups.update(groups)

                total_space += sum(
                    storage.groups[g_id].effective_space
                    for g_id in groups[:storage.Lrc.Scheme822v1.NUM_DATA_PARTS]
                )
                logger.info(
                    'Job {job_id}, task {task_id}: selected groupset {groupset}, accumulated '
                    'total space: {total_space} / {data_size}'.format(
                        job_id=self.parent_job.id,
                        task_id=self.id,
                        groupset=groups,
                        total_space=total_space,
                        data_size=data_size,
                    )
                )

            logger.info(
                'Job {job_id}, task {task_id}: performing locks on selected groupsets '
                '{groupsets}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    groupsets=groupsets,
                )
            )
            self.parent_job.groups = groupsets
            try:
                self.parent_job._set_resources()
            except Exception:
                logger.exception('Job {}: failed to set job resources'.format(self.parent_job.id))
                raise
            self.parent_job.perform_locks()

            # @determine_data_size is set to False to allow parent job to create
            # convert tasks in a standard mode
            self.parent_job.determine_data_size = False
            try:
                self.parent_job.create_tasks(processor)

                # assign data size task back to parent job
                self.parent_job.tasks.insert(0, self)
            finally:
                # set @determine_data_size to its original value
                self.parent_job.determine_data_size = True

    @staticmethod
    def _data_size(output):
        try:
            data_size = int(output)
        except ValueError:
            raise ValueError('Unexpected storage data size returned from command stdout')

        if data_size < 0:
            raise ValueError('Unexpected storage data size returned from command stdout')

        return data_size
