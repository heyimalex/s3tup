from nose.tools import raises

from s3tup.rsync import ActionPlan, RsyncConfig
from s3tup.exception import ActionConflict

class TestActionPlan:

    def test_delete(self):
        ap = ActionPlan()
        ap.add_delete('test1')
        ap.add_delete('test2')
        ap.add_delete('test1')
        r = list(ap.to_delete)
        assert len(r) == 2
        assert 'test1' in r
        assert 'test2' in r

    def test_delete_after_upload(self):
        ap = ActionPlan()
        ap.add_upload('test', 'path')
        ap.add_delete('test')
        assert len(list(ap.affected_keys)) == 1
        assert len(list(ap.to_delete)) == 0
        assert len(list(ap.to_upload)) == 1

    def test_upload_after_delete(self):
        ap = ActionPlan()
        ap.add_delete('test')
        ap.add_upload('test', 'path')
        assert len(list(ap.affected_keys)) == 1
        assert len(list(ap.to_delete)) == 0
        assert len(list(ap.to_upload)) == 1

    def test_upload_after_upload_same(self):
        ap = ActionPlan()
        ap.add_upload('test', 'path')
        ap.add_upload('test', 'path')
        assert len(list(ap.affected_keys)) == 1
        assert len(list(ap.to_upload)) == 1

    @raises(ActionConflict)
    def test_action_plan_upload_after_upload_different(self):
        ap = ActionPlan()
        ap.add_upload('test', 'path')
        ap.add_upload('test', 'different_path')

    def test_merge(self):
        ap1 = ActionPlan()
        ap2 = ActionPlan()
        ap1.add_delete('test1')
        ap2.add_upload('test2', 'path')
        ap3 = ap1 + ap2
        assert len(list(ap3.affected_keys)) == 2
        assert len(list(ap3.to_upload)) == 1
        assert len(list(ap3.to_delete)) == 1

    @raises(ActionConflict)
    def test_merge_conflict(self):
        ap1 = ActionPlan()
        ap2 = ActionPlan()
        ap1.add_redirect('test1', 'url')
        ap2.add_upload('test1', 'path')
        ap1 + ap2

# RSYNC CONFIG

def test_rsync_config_get_local_path_from_key():
    r = RsyncConfig()
    assert r._get_local_path_from_key('key') == 'key'
    r.src = 'src'
    assert r._get_local_path_from_key('key') == 'src/key'
    r.dest = 'dest'
    assert r._get_local_path_from_key('dest/key') == 'src/key'
    r.src = None
    assert r._get_local_path_from_key('dest/key') == 'key'