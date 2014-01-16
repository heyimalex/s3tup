from nose.tools import raises

from s3tup.rsync import ActionPlan, RsyncConfig
from s3tup.exception import ActionConflict

# ACTION PLAN

def test_action_plan_delete():
    ap = ActionPlan()
    ap.delete('test1')
    ap.delete('test2')
    ap.delete('test1')
    r = list(ap.to_delete)
    assert len(r) == 2
    assert 'test1' in r
    assert 'test2' in r

def test_action_plan_delete_after_upload():
    ap = ActionPlan()
    ap.upload('test', 'path')
    ap.delete('test')
    assert len(list(ap.affected_keys)) == 1
    assert len(list(ap.to_delete)) == 0
    assert len(list(ap.to_upload)) == 1

def test_action_plan_upload_after_delete():
    ap = ActionPlan()
    ap.delete('test')
    ap.upload('test', 'path')
    assert len(list(ap.affected_keys)) == 1
    assert len(list(ap.to_delete)) == 0
    assert len(list(ap.to_upload)) == 1

def test_action_plan_upload_after_upload_same():
    ap = ActionPlan()
    ap.upload('test', 'path')
    ap.upload('test', 'path')
    assert len(list(ap.affected_keys)) == 1
    assert len(list(ap.to_upload)) == 1

@raises(ActionConflict)
def test_action_plan_upload_after_upload_different():
    ap = ActionPlan()
    ap.upload('test', 'path')
    ap.upload('test', 'different_path')

def test_action_plan_merge():
    ap1 = ActionPlan()
    ap2 = ActionPlan()
    ap1.delete('test1')
    ap2.upload('test2', 'path')
    ap3 = ap1 + ap2
    assert len(list(ap3.affected_keys)) == 2
    assert len(list(ap3.to_upload)) == 1
    assert len(list(ap3.to_delete)) == 1

@raises(ActionConflict)
def test_action_plan_merge_conflict():
    ap1 = ActionPlan()
    ap2 = ActionPlan()
    ap1.redirect('test1', 'url')
    ap2.upload('test1', 'path')
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