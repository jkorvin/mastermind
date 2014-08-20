import keys
import logging
import os.path
import socket
import threading
import time
import traceback

import elliptics
import msgpack

import indexes
import inventory
from config import config
import storage
import timed_queue


logger = logging.getLogger('mm.infrastructure')

BASE_PORT = config.get('elliptics_base_port', 1024)
CACHE_DEFAULT_PORT = 9999

BASE_STORAGE_PATH = config.get('elliptics_base_storage_path', '/srv/storage/')
CACHE_DEFAULT_PATH = '/srv/cache/'

RSYNC_MODULE = config.get('restore', {}).get('rsync_use_module') and \
               config['restore'].get('rsync_module')
RSYNC_USER = config.get('restore', {}).get('rsync_user', 'rsync')

logger.info('Rsync module using: %s' % RSYNC_MODULE)
logger.info('Rsync user: %s' % RSYNC_USER)


class Infrastructure(object):

    TASK_SYNC = 'infrastructure_sync'
    TASK_UPDATE = 'infrastructure_update'
    NS_SETTINGS_SYNC = 'ns_settings_sync'

    TASK_DC_CACHE_SYNC = 'infrastructure_dc_cache_sync'

    RSYNC_CMD = ('rsync -rlHpogDt --progress '
                 '"{user}@{src_host}:{src_path}data*" "{dst_path}"')
    RSYNC_MODULE_CMD = ('rsync -av --progress '
                        '"rsync://{user}@{src_host}/{module}/{src_path}data*" '
                        '"{dst_path}"')

    def __init__(self):

        # actual init happens in 'init' method
        # TODO: return node back to constructor after wrapping
        #       all the code in a 'mastermind' package
        self.node = None
        self.meta_session = None

        self.state = {}
        self.__state_lock = threading.Lock()
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

    def init(self, node):
        self.node = node
        self.meta_session = self.node.meta_session

        self._sync_state()

        self.dc_cache = DcCacheItem(self.meta_session,
            keys.MM_DC_CACHE_IDX, keys.MM_DC_CACHE_HOST, self.__tq)
        self.dc_cache._sync_cache()

        self.hostname_cache = HostnameCacheItem(self.meta_session,
            keys.MM_HOSTNAME_CACHE_IDX, keys.MM_HOSTNAME_CACHE_HOST, self.__tq)
        self.hostname_cache._sync_cache()

        self.hosttree_cache = HostTreeCacheItem(self.meta_session,
            keys.MM_HOSTTREE_CACHE_IDX, keys.MM_HOSTTREE_CACHE_HOST, self.__tq)
        self.hosttree_cache._sync_cache()

        self.__tq.add_task_in(self.TASK_UPDATE,
            config.get('infrastructure_update_period', 300),
            self._update_state)

        self.ns_settings_idx = \
            indexes.SecondaryIndex(keys.MM_NAMESPACE_SETTINGS_IDX,
                                   keys.MM_NAMESPACE_SETTINGS_KEY_TPL,
                                   self.meta_session)

        self.ns_settings = {}
        self._sync_ns_settings()

    def get_group_history(self, group_id):
        couples_history = []
        for couple in self.state[group_id]['couples']:
            couples_history.append({'couple': couple['couple'],
                                    'timestamp': couple['timestamp']})
        nodes_history = []
        for node_set in self.state[group_id]['nodes']:

            nb_list = []
            for node in node_set['set']:
                if len(node) == 2:
                    # old version history
                    nb_list.append(node + (port_to_path(node[1]),))
                else:
                    nb_list.append(node)

            nodes_history.append({'set': nb_list,
                                  'timestamp': node_set['timestamp'],
                                  'manual': node_set.get('manual', False)})
        return {'couples': couples_history,
                'nodes': nodes_history}

    def node_backend_in_last_history_state(self, group_id, host, port, backend_id):
        if not group_id in self.state:
            raise ValueError('Group {0} history is not found'.format(group_id))

        last_node_set = self.state[group_id]['nodes'][-1]['set']
        for k in last_node_set:
            if len(k) == 2:
                # old style history record
                continue
            nb_host, nb_port, nb_backend_id = k[:3]
            if host == nb_host and port == nb_port and backend_id == nb_backend_id:
                return True

        return False

    def _sync_state(self):
        try:
            logger.info('Syncing infrastructure state')
            self.__do_sync_state()
            logger.info('Finished syncing infrastructure state')
        except Exception as e:
            logger.error('Failed to sync infrastructure state: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_SYNC,
                config.get('infrastructure_sync_period', 60),
                self._sync_state)

    def __do_sync_state(self):
        group_ids = set()
        with self.__state_lock:

            idxs = self.meta_session.find_all_indexes([keys.MM_GROUPS_IDX])
            for idx in idxs:
                data = idx.indexes[0].data

                state_group = self._unserialize(data)
                logger.debug('Fetched infrastructure item: %s' %
                              (state_group,))

                if (self.state.get(state_group['id']) and
                    state_group['id'] in storage.groups):

                    group = storage.groups[state_group['id']]

                    for nodes_state in reversed(state_group['nodes']):
                        if nodes_state['timestamp'] <= self.state[state_group['id']]['nodes'][-1]['timestamp']:
                            break

                        if nodes_state.get('manual', False):
                            nodes_set = set(nodes_state['set'])
                            for nb in group.node_backends:
                                if not (nb.node.host.addr, nb.node.port, nb.backend_id, nb.base_path) in nodes_set:
                                    logger.info('Removing {0} from group {1} due to manual group detaching'.format(nb, group.group_id))
                                    group.remove_node_backend(nb)
                        group.update_status_recursive()

                self.state[state_group['id']] = state_group
                group_ids.add(state_group['id'])

            for gid in set(self.state.keys()) - group_ids:
                logger.info('Group %d is not found in infrastructure state, '
                            'removing' % gid)
                del self.state[gid]

    @staticmethod
    def _serialize(data):
        return msgpack.packb(data)

    @staticmethod
    def _unserialize(data):
        group_state = msgpack.unpackb(data)
        group_state['nodes'] = list(group_state['nodes'])
        if not 'couples' in group_state:
            group_state['couples'] = []
        group_state['couples'] = list(group_state['couples'])
        return group_state

    @staticmethod
    def _new_group_state(group_id):
        return {
            'id': group_id,
            'nodes': [],
            'couples': [],
        }

    def _update_state(self):
        groups_to_update = []
        try:
            logger.info('Updating infrastructure state')

            logger.info('Fetching fresh infrastructure state')
            self.__do_sync_state()
            logger.info('Done fetching fresh infrastructure state')

            for g in storage.groups.keys():

                group_state = self.state.get(g.group_id,
                                             self._new_group_state(g.group_id))

                storage_nodes = tuple((nb.node.host.addr, nb.node.port, nb.backend_id, nb.base_path)
                                      for nb in g.node_backends)
                storage_couple = (tuple([group.group_id for group in g.couple])
                                  if g.couple is not None else
                                  tuple())

                if not storage_nodes:
                    logger.debug('Storage nodes list for group %d is empty, '
                                  'skipping' % (g.group_id,))
                    continue

                new_nodes = None
                new_couple = None

                with self.__state_lock:
                    if not g.group_id in self.state:
                        # add group to state only if checks succeeded
                        self.state[g.group_id] = group_state

                    cur_group_state = (group_state['nodes'] and
                                       group_state['nodes'][-1]
                                       or {'set': []})

                    state_nodes = tuple(nbs
                                        for nbs in cur_group_state['set'])
                    state_nodes_set = set(state_nodes)

                    # extended storage nodes set which includes newly seen nodes,
                    # do not discard lost nodes
                    ext_storage_nodes = (state_nodes + tuple(
                        nb for nb in storage_nodes if nb not in state_nodes_set))

                    logger.debug('Comparing %s and %s' %
                                  (ext_storage_nodes, state_nodes))

                    if set(ext_storage_nodes) != state_nodes_set:
                        logger.info('Group %d info does not match,'
                                     'last state: %s, current state: %s' %
                                     (g.group_id, state_nodes, ext_storage_nodes))
                        new_nodes = ext_storage_nodes

                    cur_couple_state = (group_state['couples'] and
                                        group_state['couples'][-1]
                                        or {'couple': tuple()})
                    state_couple = cur_couple_state['couple']

                    logger.debug('Comparing %s and %s' %
                                  (storage_couple, state_couple))

                    if storage_couple and set(state_couple) != set(storage_couple):
                        logger.info('Group %d couple does not match,'
                                     'last state: %s, current state: %s' %
                                     (g.group_id, state_couple, storage_couple))
                        new_couple = storage_couple

                    if new_nodes or new_couple:
                        try:
                            self._update_group(g.group_id, new_nodes, new_couple)
                        except Exception as e:
                            logger.error('Failed to update infrastructure state for group %s: %s\n%s' %
                                (g.group_id, e, traceback.format_exc()))
                            pass


            logger.info('Finished updating infrastructure state')
        except Exception as e:
            logger.error('Failed to update infrastructure state: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_UPDATE,
                config.get('infrastructure_update_period', 300),
                self._update_state)

    def _sync_ns_settings(self):
        try:
            logger.debug('fetching all namespace settings')
            start = time.time()
            for data in self.ns_settings_idx:
                self.__do_sync_ns_settings(data, start)
        except Exception as e:
            logger.error('Failed to sync ns settings: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.NS_SETTINGS_SYNC,
                config.get('infrastructure_ns_settings_sync_period', 60),
                self._sync_ns_settings)

    def sync_single_ns_settings(self, namespace):
        logger.debug('fetching namespace {0} settings'.format(namespace))
        self.__do_sync_ns_settings(self.ns_settings_idx[namespace], time.time())

    def __do_sync_ns_settings(self, data, start_ts):
        settings = msgpack.unpackb(data)
        logger.debug('fetched namespace settings for "{0}" '
            '({1:.4f}s)'.format(settings['namespace'], time.time() - start_ts))
        ns = settings['namespace']
        del settings['namespace']
        self.ns_settings[ns] = settings

    def set_ns_settings(self, namespace, settings):

        logger.debug('saving settings for namespace "{0}": {1}'.format(
            namespace, settings))

        settings['namespace'] = namespace
        start = time.time()

        self.ns_settings_idx[namespace] = msgpack.packb(settings)

        logger.debug('namespace "{0}" settings saved to index '
            '({1:.4f}s)'.format(namespace, time.time() - start))

        del settings['namespace']
        self.ns_settings[namespace] = settings

    def _update_group(self, group_id, new_nodes=None, new_couple=None, manual=False):
        group = self.state[group_id]
        if new_nodes is not None:
            new_nodes_state = {'set': new_nodes,
                               'timestamp': time.time()}
            if manual:
                new_nodes_state['manual'] = True

            group['nodes'].append(new_nodes_state)

        if new_couple is not None:
            new_couples_state = {'couple': new_couple,
                                 'timestamp': time.time()}
            group['couples'].append(new_couples_state)

        eid = self.meta_session.transform(keys.MM_ISTRUCT_GROUP % group_id)
        logger.info('Updating state for group %s' % group_id)
        self.meta_session.update_indexes(eid, [keys.MM_GROUPS_IDX],
                                              [self._serialize(group)])

    def detach_node(self, group, host, port, backend_id):
        with self.__state_lock:
            group_state = self.state[group.group_id]
            state_nodes = list(group_state['nodes'][-1]['set'][:])

            logger.info('{0}'.format(state_nodes))
            for i, state_node in enumerate(state_nodes):
                if len(state_node) == 2:
                    # old elliptics pre-26 record, should not be removed
                    continue
                state_host, state_port, state_backend_id = state_node[:3]
                if state_host == host and state_port == port and state_backend_id == backend_id:
                    logger.debug('Removing node backend {0}:{1}/{2} from '
                        'group {3} history state'.format(host, port, backend_id, group.group_id))
                    del state_nodes[i]
                    break
            else:
                raise ValueError('Node backend {0}:{1}/{2} not found in '
                    'group {3} history state'.format(host, port, backend_id, group.group_id))

            self._update_group(group.group_id, state_nodes, None, manual=True)


    def restore_group_cmd(self, request):
        group_id = int(request[0])
        user = request[1]
        try:
            dest = request[2]
        except IndexError:
            dest = None

        candidates = set()
        warns = []

        try:
            if not group_id in storage.groups:
                raise ValueError('Group %d is not found' % group_id)

            group = storage.groups[group_id]
            if group.couple:
                candidates.add(group.couple)
                for g in group.couple:
                    if g == group:
                        continue
                    if not group in g.couple:
                        warns.append('Group %s is not found in couple of '
                                     'group %s' % (group.group_id, g.group_id))
                    else:
                        candidates.add(g.couple)
            else:
                candidates.update(c for c in storage.couples if group in c)

            if not candidates:
                raise ValueError('Couples containing group %s are not found' %
                                 group_id)

            couple = candidates.pop()
            if len(candidates) > 0:
                warns.append('More than one couple candidate '
                             'for group restoration: %s' % (candidates,))
                warns.append('Selected couple: %s' % (couple,))

            group_candidates = []
            for g in couple:
                if g == group:
                    continue
                if g.status != storage.Status.BAD:
                    warns.append('Cannot use group %s, status: %s '
                                 '(expected %s)' %
                                 (g.group_id, g.status, storage.Status.BAD))
                else:
                    group_candidates.append(g)

            if not group_candidates:
                raise ValueError('No symmetric groups to restore from')

            group_candidates = filter(lambda g: len(g.node_backends) == 1, group_candidates)

            if not group_candidates:
                raise ValueError('No symmetric groups with one node backend found, '
                                 'multiple nodes group restoration is not supported')

            def alive_keys(g):
                stat = g.get_stat()
                return stat.files

            group_candidates.sort(key=alive_keys, reverse=True)

            source_group = group_candidates[0]
            source_node_backend = source_group.node_backends[0]

            state = self.get_group_history(group.group_id)['nodes'][-1]['set']
            if len(state) > 1:
                raise ValueError('Restoring group has more than one node backend, '
                    'multiple node backends group restoration is not supported')

            if len(state[0]) == 2:
                raise ValueError('Restoring group has no valid history records')

            addr, port, backend_id, path = state[0][:3]

            if (dest and
                (group.node_backends[0].node.host.addr != addr or
                 group.node_backends[0].node.port != port or
                 group.node_backends[0].backend_id != backend_id)):
                warns.append('Restoring group history state does not match '
                             'current state, history will be used for '
                             'path construction: history {0}:{1}/{2}, current {3}' %
                             (addr, port, backend_id, group.node_backends[0]))

            if len(source_group.node_backends) > 1 or len(state) > 1:
                raise ValueError('Do not know how to restore group '
                                 'with more than one node backend')

            logger.info('Constructing restore cmd for group %s '
                         'from group %s, (%s)' %
                         (group.group_id, source_group, source_node_backend))
            warns.append('Source group %s (%s)' % (source_group, source_node_backend))

            cmd = self.move_group_cmd(
                src_host=source_node_backend.node.host.addr,
                src_port=source_node_backend.base_path,
                dst_path=path,
                user=user)

        except ValueError as e:
            warns.append(e.message)
            logger.info('Restore cmd for group %s failed, warns: %s' %
                         (group_id, warns))
            return '', 0, 0, '', warns

        logger.info('Restore cmd for group %s on %s, warns: %s, cmd %s' %
                     (group_id, addr, warns, cmd))
        return addr, port, backend_id, cmd, warns

    def move_group_cmd(self, src_host, src_port=None, dst_port=None, src_path=None, dst_path=None, user=None):
        cmd_src_path = self.node_path(path=src_path, port=src_port)
        if RSYNC_MODULE:
            cmd = self.RSYNC_MODULE_CMD.format(
                user=RSYNC_USER,
                module=RSYNC_MODULE,
                src_host=src_host,
                src_path=cmd_src_path.replace(BASE_STORAGE_PATH, ''),
                dst_path=self.node_path(path=dst_path, port=dst_port))
        else:
            cmd = self.RSYNC_CMD.format(
                user=user,
                src_host=src_host,
                src_path=cmd_src_path,
                dst_path=self.node_path(path=dst_path, port=dst_port))
        return cmd

    @staticmethod
    def node_path(path=None, port=None):
        if not path and not port:
            raise ValueError('Either path or port should be specified')
        return path or port_to_path(port)

    def start_node_cmd(self, request):

        host, port = request[:2]

        # TODO: Fix family value
        cmd = inventory.node_start_command(host, port, 2)

        if cmd is None:
            raise RuntimeError('Node start command is not provided '
                'by inventory implementation')

        logger.info('Command for starting elliptics node {0}:{1} '
            'was requested: {2}'.format(host, port, cmd))

        return cmd

    def shutdown_node_cmd(self, request):

        host, port = request[:2]

        node_addr = '{0}:{1}'.format(host, port)

        if not node_addr in storage.nodes:
            raise ValueError("Node {0} doesn't exist".format(node_addr))

        node = storage.nodes[node_addr]

        cmd = inventory.node_shutdown_command(node.host, node.port, node.family)
        logger.info('Command for shutting down elliptics node {0} '
            'was requested: {1}'.format(node_addr, cmd))

        return cmd

    def enable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id)

        cmd = inventory.enable_node_backend_cmd(host, port, family, backend_id)

        if cmd is None:
            raise RuntimeError('Node backend start command is not provided '
                'by inventory implementation')

        logger.info('Command for starting elliptics node {0} '
            'was requested: {1}'.format(nb_addr, cmd))

        return cmd


    def disable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:3]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if not nb_addr in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = inventory.disable_node_backend_command(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info('Command for shutting down elliptics node backend {0} '
            'was requested: {1}'.format(nb_addr, cmd))

        return cmd

    def get_dc_by_host(self, host):
        return self.dc_cache[host]

    def get_hostname_by_addr(self, addr):
        return self.hostname_cache[addr]

    def get_host_tree(self, hostname):
        return self.hosttree_cache[hostname]


class CacheItem(object):

    def __init__(self, meta_session, idx_key, key_key, tq):
        self.meta_session = meta_session.clone()
        self.idx = indexes.SecondaryIndex(idx_key, key_key, self.meta_session)
        self.__tq = tq
        self.cache = {}

        for attr in ['taskname', 'logprefix', 'sync_period', 'key_expire_time']:
            if getattr(self, attr) is None:
                raise AttributeError('Set "{0}" attribute explicitly in your '
                                 'class instance'.format(attr))

    def get_value(self, key):
        raise NotImplemented('Method "get_value" should be implemented in '
                             'derived class')

    def _sync_cache(self):
        try:
            logger.info(self.logprefix + 'syncing')
            for data in self.idx:
                data = msgpack.unpackb(data)

                logger.debug(self.logprefix + 'Fetched item: %s' %
                              (data,))

                try:
                    self.cache[data['key']] = data
                except KeyError:
                    pass

            logger.info(self.logprefix + 'Finished syncing')
        except Exception as e:
            logger.error(self.logprefix + 'Failed to sync: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.taskname,
                self.sync_period, self._sync_cache)

    def _update_cache_item(self, key, val):
        cache_item = {'key': key,
                      'val': val,
                      'ts': time.time()}
        logger.info(self.logprefix + 'Updating item for key %s '
                                      'to value %s' % (key, val))
        self.idx[key] = msgpack.packb(cache_item)
        self.cache[key] = cache_item

    def __getitem__(self, key):
        try:
            cache_item = self.cache[key]
            if cache_item['ts'] + self.key_expire_time < time.time():
                logger.debug(self.logprefix + 'Item for key %s expired' % (key,))
                raise KeyError
            val = cache_item['val']
        except KeyError:
            logger.debug(self.logprefix + 'Fetching value for key %s from source' % (key,))
            try:
                req_start = time.time()
                val = self.get_value(key)
                logger.info(self.logprefix + 'Fetched value for key %s from source: %s' %
                             (key, val))
            except Exception as e:
                req_time = time.time() - req_start
                logger.error(self.logprefix + 'Failed to fetch value for key {0} (time: {1:.5f}s): {2}\n{3}'.format(
                    key, req_time, str(e), traceback.format_exc()))
                raise
            self._update_cache_item(key, val)

        return val


class DcCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_dc_cache_sync'
        self.logprefix = 'dc cache: '
        self.sync_period = config.get('infrastructure_dc_cache_update_period', 150)
        self.key_expire_time = config.get('infrastructure_dc_cache_valid_time', 604800)
        super(DcCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return inventory.get_dc_by_host(key)


class HostnameCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hostname_cache_sync'
        self.logprefix = 'hostname cache: '
        self.sync_period = config.get('infrastructure_hostname_cache_update_period', 600)
        self.key_expire_time = config.get('infrastructure_hostname_cache_valid_time', 604800)
        super(HostnameCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return socket.gethostbyaddr(key)[0]


class HostTreeCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hosttree_cache_sync'
        self.logprefix = 'hosttree cache: '
        self.sync_period = config.get('infrastructure_hosttree_cache_update_period', 600)
        self.key_expire_time = config.get('infrastructure_hosttree_cache_valid_time', 604800)
        super(HostTreeCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return inventory.get_host_tree(key)


infrastructure = Infrastructure()


def port_to_path(port):
    assert port >= BASE_PORT
    if port == CACHE_DEFAULT_PORT:
        return CACHE_DEFAULT_PATH
    return os.path.join(BASE_STORAGE_PATH, port_to_dir(port) + '/')


def port_to_dir(port):
    return str(port - BASE_PORT)
