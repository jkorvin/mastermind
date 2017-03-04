#!/usr/bin/env python
import json
import sys

import pymongo


CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file {0}: {1}'.format(CONFIG_PATH, e))


def get_mongo_client():
    if not config.get('metadata', {}).get('url', ''):
        raise ValueError('Mongo db url is not set')
    return pymongo.mongo_replica_set_client.MongoReplicaSetClient(config['metadata']['url'])


def update_cleaning_details(collection, dry_run):
    jobs = collection.find({
        'status': 'executing',
    })

    print 'Found {0} executing jobs'.format(jobs.count())

    for job in jobs:
        update = False

        for task in job['tasks']:
            if task['status'] == 'executing':
                if not task.get('cleaning_details', {}).get('status_after_clean_step'):
                    task.setdefault('cleaning_details', {})['status_after_clean_step'] = 'completed'
                    update = True

                if (
                    task['type'] in ('external_storage_data_size', 'rsync_backend_task')
                    and
                    not task.get('cleaning_details', {}).get('preparation')
                ):
                    task.setdefault('cleaning_details', {})['preparation'] = None
                    update = True

        if update:
            print 'Update job: {0}'.format(job['id'])

            if not dry_run:
                res = collection.update({'id': job['id']}, job)
                if res['ok'] != 1:
                    raise RuntimeError('job was not updated, info {0}'.format(res))


if __name__ == '__main__':

    COMMANDS = ('update', 'show-to-update')
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print "Usage: {0} ({1})".format(sys.argv[0], '|'.join(COMMANDS))
        sys.exit(1)

    COLLECTION_NAME = 'jobs'

    db_name = config.get('metadata', {}).get('jobs', {}).get('db', '')
    if not db_name:
        print 'Namespaces database name is not found in config'
        sys.exit(1)

    mc = get_mongo_client()

    collection = mc[db_name][COLLECTION_NAME]
    update_cleaning_details(collection, sys.argv[1] != 'update')

    print 'Successfully updated cleaning details in collection "{0}"'.format(COLLECTION_NAME)
