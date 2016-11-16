from jobs.job_types import TaskTypes

from node_stop import NodeStopTask
from minion_cmd import MinionCmdTask
from history_remove_node import HistoryRemoveNodeTask
from recover_group_dc import RecoverGroupDcTask
from node_backend_defrag import NodeBackendDefragTask
from couple_defrag_state_check import CoupleDefragStateCheckTask
from rsync_backend import RsyncBackendTask
from create_group import CreateGroupTask
from remove_group import RemoveGroupTask
from dnet_client_backend_cmd import DnetClientBackendCmdTask
from wait_groupset_state import WaitGroupsetStateTask
from write_meta_key import WriteMetaKeyTask
from change_couple_frozen_status import ChangeCoupleFrozenStatusTask
from external_storage_data_size import ExternalStorageDataSizeTask
from write_external_storage_mapping import WriteExternalStorageMappingTask
from change_couple_settings import ChangeCoupleSettingsTask


class TaskFactory(object):

    @staticmethod
    def make_task(data, job):
        Tasks = {
            TaskTypes.TYPE_NODE_STOP_TASK:
                NodeStopTask,
            TaskTypes.TYPE_MINION_CMD:
                MinionCmdTask,
            TaskTypes.TYPE_HISTORY_REMOVE_NODE:
                HistoryRemoveNodeTask,
            TaskTypes.TYPE_RECOVER_DC_GROUP_TASK:
                RecoverGroupDcTask,
            TaskTypes.TYPE_NODE_BACKEND_DEFRAG_TASK:
                NodeBackendDefragTask,
            TaskTypes.TYPE_COUPLE_DEFRAG_STATE_CHECK_TASK:
                CoupleDefragStateCheckTask,
            TaskTypes.TYPE_RSYNC_BACKEND_TASK:
                RsyncBackendTask,
            TaskTypes.TYPE_CREATE_GROUP:
                CreateGroupTask,
            TaskTypes.TYPE_REMOVE_GROUP:
                RemoveGroupTask,
            TaskTypes.TYPE_DNET_CLIENT_BACKEND_CMD:
                DnetClientBackendCmdTask,
            TaskTypes.TYPE_WAIT_GROUPSET_STATE:
                WaitGroupsetStateTask,
            TaskTypes.TYPE_WRITE_META_KEY:
                WriteMetaKeyTask,
            TaskTypes.TYPE_CHANGE_COUPLE_FROZEN_STATUS:
                ChangeCoupleFrozenStatusTask,
            TaskTypes.TYPE_EXTERNAL_STORAGE_DATA_SIZE:
                ExternalStorageDataSizeTask,
            TaskTypes.TYPE_WRITE_EXTERNAL_STORAGE_MAPPING:
                WriteExternalStorageMappingTask,
            TaskTypes.TYPE_CHANGE_COUPLE_SETTINGS:
                ChangeCoupleSettingsTask,
        }
        task_type = data.get(type)
        if task_type not in Tasks:
            raise ValueError('Unknown task type {0}'.format(task_type))
        return Tasks.get(task_type).from_data(data, job)
