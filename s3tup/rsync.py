from binascii import hexlify
import hashlib
import logging
import os

from s3tup.exception import ActionConflict
import s3tup.utils as utils
import s3tup.constants as constants

log = logging.getLogger('s3tup.rsync')


class ActionPlan(object):

    """Maintains a set of actions to be executed on keys.

    Buckets know how to execute action plans, and action
    plans know when there are conflicting actions on keys.
    Actions are added to action plans

    """

    def __init__(self):
        self._actions = {}

    # Ugly... but works and passes tests
    def _add_action(self, key, action_type, **kwargs):

        new_action = {'type': action_type}
        new_action.update(kwargs)
        old_action = self._actions.pop(key, None)

        if old_action is None:
            self._actions[key] = new_action
        elif old_action == new_action:
            self._actions[key] = old_action
        elif old_action['type'] == 'delete':
            self._actions[key] = new_action
        elif new_action['type'] == 'delete':
            self._actions[key] = old_action
        else:
            raise ActionConflict(key, new_action, old_action)

    def remove_actions(self, *action_types):
        for k, v in list(self._actions.items()):
            if v['type'] in action_types:
                self._actions.pop(k)

    def add_delete(self, key):
        self._add_action(key, 'delete')

    def add_sync(self, key):
        self._add_action(key, 'sync')

    def add_redirect(self, key, url):
        self._add_action(key, 'redirect', url=url)

    def add_upload(self, key, path):
        self._add_action(key, 'upload', path=path)

    @property
    def affected_keys(self):
        return self._actions.keys()

    @property
    def to_upload(self):
        for k, v in self._actions.items():
            if v['type'] == 'upload':
                yield k, v['path']

    @property
    def to_redirect(self):
        for k, v in self._actions.items():
            if v['type'] == 'redirect':
                yield k, v['url']

    @property
    def to_delete(self):
        for k, v in self._actions.items():
            if v['type'] == 'delete':
                yield k

    @property
    def to_sync(self):
        for k, v in self._actions.items():
            if v['type'] == 'sync':
                yield k

    def __add__(self, other):
        new = ActionPlan()
        for old in (other, self):
            for key, v in old._actions.items():
                if v['type'] == 'redirect':
                    new.add_redirect(key, v['url'])
                if v['type'] == 'upload':
                    new.add_upload(key, v['path'])
                if v['type'] == 'delete':
                    new.add_delete(key)
                if v['type'] == 'sync':
                    new.add_sync(key)
        return new

    def __iadd__(self, other):
        return self.__add__(other)


class RsyncPlanner(object):
    """Container for RsyncConfigs."""

    def __init__(self, rsync_configs=None):
        self.configs = rsync_configs or []

    def plan(self, remote_keys):
        plan = ActionPlan()
        for config in self.configs:
            plan += config.plan(remote_keys)
        return plan


class RsyncConfig(object):

    def __init__(self, src=None, dest=None, delete=False, matcher=None):
        self.src = src
        self.dest = dest
        self.delete = delete
        self.matcher = matcher or utils.Matcher()

    def plan(self, remote_keys):

        remote_key_names = set(remote_keys.keys())

        new = set()
        removed = set()
        modified = set()
        unmodified = set()

        for k in self._get_local_key_names():
            if k not in remote_key_names:
                new.add(k)
            else:
                if (self._is_unmodified(remote_keys[k])):
                    unmodified.add(k)
                else:
                    modified.add(k)
        removed = remote_key_names - (modified | unmodified)

        plan = ActionPlan()
        for k in new | modified:
            plan.add_upload(k, self._get_local_path_from_key(k))
        for k in unmodified:
            plan.add_sync(k)
        if self.delete:
            for k in removed:
                plan.add_delete(k)
        return plan

    def _get_local_key_names(self):
        src = self.src or '.'
        dest = self.dest or '.'
        for path in utils.os_walk_relative(src):
            if not self.matcher.matches(path):
                continue
            yield os.path.normpath(os.path.join(dest, path))

    def _get_local_path_from_key(self, key):
        src = self.src or '.'
        dest = self.dest or '.'
        return os.path.normpath(os.path.join(src, os.path.relpath(key, dest)))

    def _is_unmodified(self, s3_key):
        local_path = self._get_local_path_from_key(s3_key.name)
        with open(local_path, 'rb') as f:
            if utils.f_sizeof(f) != s3_key.size:
                return False
            if not '-' in s3_key.md5:
                local_md5 = hexlify(utils.f_md5(f))
            else:
                chunks = utils.f_chunk(f, constants.MULTIPART_PART_SIZE)
                m = hashlib.md5()
                for chunk in chunks:
                    m.update(utils.f_md5(chunk))
                local_md5 = "{}-{}".format(hexlify(m.digest()), len(chunks))
            return local_md5 == s3_key.md5
