from binascii import hexlify
import hashlib
import logging
import os

from s3tup.exception import ActionConflict
import s3tup.utils as utils

log = logging.getLogger('s3tup.rsync')

class ActionPlan(object):
    """ Stores a set of actions to be executed on keys.

    Buckets know how to execute action plans, and action
    plans know when there are conflicting actions on keys.
    """

    def __init__(self):
        self._actions = {}

    # Ugly... but works and passes tests
    def _add_action(self, action_type, key, *args):
        new_val = [action_type,]+list(args)
        old_val = self._actions.pop(key, None)

        if old_val == None:
            self._actions[key] = new_val
        elif old_val[0] == 'delete':
            if action_type != 'delete':
                # don't complain if delete is overwritten
                pass
            self._actions[key] = new_val
        else:
            if action_type == 'delete':
                self._actions[key] = old_val
            else:
                if new_val == old_val:
                    self._actions[key] = old_val
                else:
                    msg = "Conflicting actions set for key '{}':\n".format(key)
                    for v in (new_val, old_val):
                        if v[0] in ("delete", "sync"):
                            msg += '{}, '.format(v[0])
                        else:
                            msg += '{} -> {}, '.format(v[0], v[1])
                    msg = msg[:-2]
                    raise ActionConflict(msg)

    def remove_actions(self, *action_types):
        toremove = []
        for key,v in self._actions.items():
            if v[0] in action_types:
                toremove.append(key)
        for k in toremove:
            self._actions.pop(k) #

    def delete(self, key):
        self._add_action('delete', key)

    def sync(self, key):
        self._add_action('sync', key)

    def redirect(self, key, url):
        self._add_action('redirect', key, url)

    def upload(self, key, path):
        self._add_action('upload', key, path)

    @property
    def affected_keys(self):
        return self._actions.keys()

    @property
    def to_upload(self):
        for k,v in self._actions.items():
            if v[0] == 'upload':
                yield k, v[1]

    @property
    def to_redirect(self):
        for k,v in self._actions.items():
            if v[0] == 'redirect':
                yield k, v[1]

    @property
    def to_delete(self):
        for k,v in self._actions.items():
            if v[0] == 'delete':
                yield k

    @property
    def to_sync(self):
        for k,v in self._actions.items():
            if v[0] == 'sync':
                yield k

    def __add__(self, other):
        new = ActionPlan()
        for old in (other, self):
            for key,v in old._actions.items():
                if v[0] == 'redirect':
                    new.redirect(key, v[1])
                if v[0] == 'upload':
                    new.upload(key, v[1])
                if v[0] == 'delete':
                    new.delete(key)
                if v[0] == 'sync':
                    new.sync(key)
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
            plan.upload(k, self._get_local_path_from_key(k))
        for k in unmodified:
            plan.sync(k)
        if self.delete:
            for k in removed:
                plan.delete(k)
        return plan

    def _is_unmodified(self, s3_key):
        local_path = self._get_local_path_from_key(s3_key.name)
        with open(local_path, 'rb') as f:
            if utils.f_sizeof(f) != s3_key.size:
                return False
            if not '-' in s3_key.md5:
                local_md5 = hexlify(utils.f_md5(f))
            else:
                chunks = utils.f_chunk(f, 5242880)
                m = hashlib.md5()
                for chunk in chunks:
                    m.update(utils.f_md5(chunk))
                local_md5 = "{}-{}".format(hexlify(m.digest()), len(chunks))
            return local_md5 == s3_key.md5        

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