
class JobTypes(object):
    TYPE_MOVE_JOB = 'move_job'
    TYPE_RECOVER_DC_JOB = 'recover_dc_job'
    TYPE_COUPLE_DEFRAG_JOB = 'couple_defrag_job'
    TYPE_RESTORE_GROUP_JOB = 'restore_group_job'
    TYPE_MAKE_LRC_GROUPS_JOB = 'make_lrc_groups_job'
    TYPE_ADD_LRC_GROUPSET_JOB = 'add_lrc_groupset_job'
    TYPE_CONVERT_TO_LRC_GROUPSET_JOB = 'convert_to_lrc_groupset_job'
    TYPE_TTL_CLEANUP_JOB = 'ttl_cleanup_job'
    TYPE_BACKEND_CLEANUP_JOB = 'backend_cleanup_job'
    TYPE_BACKEND_MANAGER_JOB = 'backend_manager_job'
    TYPE_S3_CLEANUP_JOB = 's3_cleanup_job'

    AVAILABLE_TYPES = (
        TYPE_MOVE_JOB,
        TYPE_RECOVER_DC_JOB,
        TYPE_COUPLE_DEFRAG_JOB,
        TYPE_RESTORE_GROUP_JOB,
        TYPE_MAKE_LRC_GROUPS_JOB,
        TYPE_BACKEND_CLEANUP_JOB,
        TYPE_BACKEND_MANAGER_JOB,
        TYPE_S3_CLEANUP_JOB,
    )


class TaskTypes(object):
    TYPE_MINION_CMD = 'minion_cmd'
    TYPE_NODE_STOP_TASK = 'node_stop_task'
    TYPE_RECOVER_DC_GROUP_TASK = 'recover_dc_group_task'
    TYPE_HISTORY_REMOVE_NODE = 'history_remove_node'
    TYPE_NODE_BACKEND_DEFRAG_TASK = 'node_backend_defrag_task'
    TYPE_COUPLE_DEFRAG_STATE_CHECK_TASK = 'couple_defrag_state_check'
    TYPE_RSYNC_BACKEND_TASK = 'rsync_backend_task'
    TYPE_CREATE_GROUP = 'create_group'
    TYPE_REMOVE_GROUP = 'remove_group'
    TYPE_WRITE_META_KEY = 'write_meta_key'
    TYPE_DNET_CLIENT_BACKEND_CMD = 'dnet_client_backend_cmd'
    TYPE_WAIT_GROUPSET_STATE = 'wait_groupset_state'
    TYPE_CHANGE_COUPLE_FROZEN_STATUS = 'change_couple_frozen_status'
    TYPE_EXTERNAL_STORAGE_DATA_SIZE = 'external_storage_data_size'
    TYPE_WRITE_EXTERNAL_STORAGE_MAPPING = 'write_external_storage_mapping'
    TYPE_CHANGE_COUPLE_SETTINGS = 'change_couple_settings'
    TYPE_S3_SHARD_CLEANUP_TASK = 's3_db_shard_cleanup_task'
