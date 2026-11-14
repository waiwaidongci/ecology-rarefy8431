import json,os,time,threading,subprocess,sys,pathlib
import pytest
from src.config import settings
from src.models import DiversityRun,RunSample,AuditLog,User
from src.services import runs as run_service
from src.services import samples as sample_store
from .conftest import CONFIG,make_run,run_thread,_drive,wait_until,progress,saved,sample_rows,baseline,token

BACKEND=str(pathlib.Path(__file__).resolve().parents[1])

@pytest.fixture(autouse=True)
def fast_chunks(monkeypatch):
 monkeypatch.setattr(settings,'ANALYSIS_CHUNK_SIZE',4)
 monkeypatch.setattr(settings,'ANALYSIS_CONTINUE_AFTER',600)

@pytest.fixture()
def admin(db,study):
 return db.query(User).filter_by(role='admin').one()

def test_worker_matches_one_shot(db,study,enqueued):
 expected=baseline(study)
 run=make_run(db,study)
 _drive(run.id,0)
 status,done,total=progress(db,run.id)
 assert status=='succeeded' and total==CONFIG['bootstrap']*2 and done==total
 saved_run=saved(db,run.id)
 assert saved_run.result==expected
 artifact=pathlib.Path(settings.ARTIFACT_DIR,f'diversity-{run.id}.json')
 assert json.loads(artifact.read_text())==expected
 assert len(sample_rows(db,run.id))==CONFIG['bootstrap']*2

def test_pause_mid_run_then_resume_is_identical(db,study,enqueued,admin,monkeypatch):
 monkeypatch.setattr(settings,'ANALYSIS_STEP_DELAY',0.02)
 expected=baseline(study)
 run=make_run(db,study)
 worker=run_thread(run.id,0)
 assert wait_until(lambda:progress(db,run.id)[1]>0)
 paused=run_service.pause_run(db,run.id,admin)
 assert paused['status']=='paused'
 worker.join(15);assert not worker.is_alive()
 status,done,total=progress(db,run.id)
 assert status=='paused' and 0<done<total
 assert saved(db,run.id).result=={}
 assert not pathlib.Path(settings.ARTIFACT_DIR,f'diversity-{run.id}.json').exists()
 time.sleep(0.4)
 assert progress(db,run.id)[1]==done  # 暂停确认后，旧计算不再改进度
 resumed=run_service.resume_run(db,run.id,admin)
 assert resumed['status']=='running' and resumed['epoch']==paused['epoch']+1
 assert enqueued[-1]==(run.id,resumed['epoch'])
 _drive(*enqueued[-1])
 status,done,total=progress(db,run.id)
 assert status=='succeeded' and done==total
 assert saved(db,run.id).result==expected  # 续算与一次算完逐项相同
 rows=sample_rows(db,run.id)
 assert len(rows)==CONFIG['bootstrap']*2  # 不重复计入
 for tid in study['transect_ids'][:2]:
  assert sorted(r.replicate_index for r in rows if r.transect_id==tid)==list(range(CONFIG['bootstrap']))

def test_pause_fences_stale_epoch(db,study,enqueued,admin):
 from src.worker import _guarded
 run=make_run(db,study)
 run_service.pause_run(db,run.id,admin)  # 排队中直接暂停：epoch 0→1
 assert not _guarded(db,run.id,0,completed_replicates=5)  # 旧epoch写入被拒绝
 db.rollback()
 _drive(run.id,0)  # 旧队列任务不再启动
 status,done,total=progress(db,run.id)
 assert status=='paused' and done==0 and saved(db,run.id).result=={}
 resumed=run_service.resume_run(db,run.id,admin)
 _drive(*enqueued[-1])
 assert progress(db,run.id)[0]=='succeeded'
 assert saved(db,run.id).result==baseline(study)

def test_double_resume_advances_single_copy(db,study,enqueued,admin,monkeypatch):
 monkeypatch.setattr(settings,'ANALYSIS_STEP_DELAY',0.02)
 expected=baseline(study)
 run=make_run(db,study)
 worker=run_thread(run.id,0)
 assert wait_until(lambda:progress(db,run.id)[1]>0)
 run_service.pause_run(db,run.id,admin);worker.join(15)
 enqueued.clear()
 errors=[]
 def resume():
  from src.config.database import SessionLocal
  try:
   with SessionLocal() as session:run_service.resume_run(session,run.id,admin)
  except Exception as e:errors.append(e)
 threads=[threading.Thread(target=resume) for _ in range(2)]
 [t.start() for t in threads];[t.join() for t in threads]
 assert not errors and len(enqueued)==2
 assert len({ep for _,ep in enqueued})==2  # 两次续算各持一个epoch
 workers=[run_thread(run.id,ep) for _,ep in enqueued]
 [t.join(30) for t in workers]
 assert all(not t.is_alive() for t in workers)
 status,done,total=progress(db,run.id)
 assert status=='succeeded' and done==total
 assert saved(db,run.id).result==expected
 assert len(sample_rows(db,run.id))==CONFIG['bootstrap']*2  # 并发续算也只推进一份、不重复计入

def test_crash_keeps_committed_samples_and_recovers(db,study,enqueued,admin,monkeypatch):
 monkeypatch.setattr(settings,'ANALYSIS_STEP_DELAY',0.02)
 import src.worker
 expected=baseline(study)
 run=make_run(db,study)
 original=src.worker.replicate_row
 calls={'n':0}
 def crashing(*args):
  calls['n']+=1
  if calls['n']>12:raise RuntimeError('simulated process death')
  return original(*args)
 monkeypatch.setattr(src.worker,'replicate_row',crashing)
 worker=run_thread(run.id,0);worker.join(15);assert not worker.is_alive()
 status,done,total=progress(db,run.id)
 assert status=='running' and 0<done<total  # 中断后已提交样本保留、状态可续算
 assert db.query(AuditLog).filter_by(action='diversity.interrupted').count()==1
 monkeypatch.setattr(src.worker,'replicate_row',original)
 run_service.resume_run(db,run.id,admin)
 _drive(*enqueued[-1])
 status,done,total=progress(db,run.id)
 assert status=='succeeded' and done==total
 assert saved(db,run.id).result==expected
 assert len(sample_rows(db,run.id))==CONFIG['bootstrap']*2

def test_killed_process_recovers(db,study,enqueued,admin):
 expected=baseline(study)
 run=make_run(db,study)
 env=dict(os.environ,ANALYSIS_STEP_DELAY='0.05',ANALYSIS_CHUNK_SIZE='3')
 code=f"import sys;sys.path.insert(0,{BACKEND!r});from src.worker import analyze_run;analyze_run({run.id},0)"
 proc=subprocess.Popen([sys.executable,'-c',code],env=env)
 try:assert wait_until(lambda:progress(db,run.id)[1]>0,timeout=60)
 finally:proc.kill()
 proc.wait(10);assert proc.returncode==-9
 status,done,total=progress(db,run.id)
 assert status=='running' and 0<done<total  # 进程被kill后进度不丢
 run_service.resume_run(db,run.id,admin)
 _drive(*enqueued[-1])
 status,done,total=progress(db,run.id)
 assert status=='succeeded' and done==total
 assert saved(db,run.id).result==expected
 assert len(sample_rows(db,run.id))==CONFIG['bootstrap']*2

def test_params_change_starts_fresh_study(db,study,enqueued,admin,monkeypatch):
 monkeypatch.setattr(settings,'ANALYSIS_STEP_DELAY',0.02)
 run_a=make_run(db,study)
 worker=run_thread(run_a.id,0)
 assert wait_until(lambda:progress(db,run_a.id)[1]>0)
 run_service.pause_run(db,run_a.id,admin);worker.join(15)
 kept=sample_store.count(db,run_a.id);assert kept>0
 other=make_run(db,study,seed=100)  # 参数改变另建研究
 _drive(other.id,0)
 assert progress(db,other.id)[0]=='succeeded'
 assert sample_store.count(db,run_a.id)==kept  # 旧研究样本不被混用
 assert sample_store.count(db,other.id)==CONFIG['bootstrap']*2
 assert saved(db,other.id).result['fingerprint']!=saved(db,run_a.id).params_fingerprint
 same=make_run(db,study)  # 同参数另建研究：独立样本、结果确定一致
 _drive(same.id,0)
 assert saved(db,same.id).result==baseline(study)
 assert sample_store.count(db,run_a.id)==kept and sample_store.count(db,same.id)==CONFIG['bootstrap']*2

def test_no_intervals_before_completion(db,study,enqueued,admin,client,users,monkeypatch):
 monkeypatch.setattr(settings,'ANALYSIS_STEP_DELAY',0.05)
 run=make_run(db,study)
 worker=run_thread(run.id,0)
 assert wait_until(lambda:progress(db,run.id)[1]>0)
 detail=client.get(f'/runs/{run.id}',headers=token(users['analyst'])).json()
 assert detail['status']=='running' and detail['result']=={}
 assert detail['completed_replicates']<detail['total_replicates']
 assert set(detail['sample_progress'])<={str(t) for t in study['transect_ids']}
 run_service.pause_run(db,run.id,admin);worker.join(15)
 detail=client.get(f'/runs/{run.id}',headers=token(users['viewer'])).json()
 assert detail['status']=='paused' and detail['result']=={}  # 完成前不出区间

def test_pause_resume_authorization_and_states(db,study,enqueued,client,users):
 run=make_run(db,study)
 assert client.post(f'/runs/{run.id}/pause').status_code==401
 assert client.post(f'/runs/{run.id}/pause',headers=token(users['viewer'])).status_code==403
 assert client.post(f'/runs/{run.id}/resume',headers=token(users['viewer'])).status_code==403
 assert client.post('/runs/9999/pause',headers=token(users['analyst'])).status_code==404
 assert client.post(f'/runs/{run.id}/pause',headers=token(users['analyst'])).status_code==200
 assert client.post(f'/runs/{run.id}/pause',headers=token(users['analyst'])).status_code==409  # 已暂停不可重复暂停
 assert client.post(f'/runs/{run.id}/resume',headers=token(users['analyst'])).status_code==200
 _drive(*enqueued[-1])
 assert progress(db,run.id)[0]=='succeeded'
 assert client.post(f'/runs/{run.id}/pause',headers=token(users['admin'])).status_code==409
 assert client.post(f'/runs/{run.id}/resume',headers=token(users['admin'])).status_code==409

def test_create_run_via_api_and_audit(db,study,enqueued,client,users):
 payload={'name':'API研究','survey_id':study['survey_id'],'transect_ids':study['transect_ids'],**CONFIG}
 created=client.post('/runs',json=payload,headers=token(users['analyst']))
 assert created.status_code==201 and enqueued[-1][1]==0
 assert db.query(AuditLog).filter_by(action='diversity.queued').count()==1
 _drive(*enqueued[-1])
 assert db.query(AuditLog).filter_by(action='diversity.succeeded').count()==1
