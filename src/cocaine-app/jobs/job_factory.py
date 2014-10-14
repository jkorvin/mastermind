from job_types import JobTypes
from move import MoveJob
from recover_dc import RecoverDcJob


class JobFactory(object):

    @staticmethod
    def make_job(data):
        job_type = data.get('type', None)
        if job_type == JobTypes.TYPE_MOVE_JOB:
            return MoveJob.from_data(data)
        elif job_type == JobTypes.TYPE_RECOVER_DC_JOB:
            return RecoverDcJob.from_data(data)
        raise ValueError('Unknown job type {0}'.format(job_type))
