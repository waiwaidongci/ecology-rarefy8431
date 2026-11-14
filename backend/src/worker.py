import json,time,logging
from pathlib import Path
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import update,select,func
from src.config.database import SessionLocal
from src.config import settings
from src.models import DiversityRun,Survey,RunSample
from src.constants.enums import RunStatus
from src.services.transects import list_transects
from src.services.analysis import prepare,assemble
from src.services.bootstrap import replicate_row,intervals_from_rows
from src.services import samples as sample_store
from src.services.audit import record
from src.utils.serialize import row
log=logging.getLogger('ecology.worker')
app=Celery('ecology_rarefy',broker=settings.REDIS_URL,backend=settings.REDIS_URL)
app.conf.update(task_track_started=True,worker_prefetch_multiplier=1,task_acks_late=True)

def enqueue_run(run_id,epoch):analyze_run.delay(run_id,epoch)

def _claim(db,run_id,epoch):
 # A task only starts when it holds the current epoch and the run is live.
 return db.execute(update(DiversityRun).where(DiversityRun.id==run_id,DiversityRun.epoch==epoch,DiversityRun.status.in_([RunStatus.QUEUED,RunStatus.RUNNING])).values(status=RunStatus.RUNNING)).rowcount==1

def _guarded(db,run_id,epoch,**values):
 # Every write of an executing worker carries its epoch: once a pause or a newer
 # resume is confirmed, the old worker's transaction matches nothing and rolls back.
 return db.execute(update(DiversityRun).where(DiversityRun.id==run_id,DiversityRun.epoch==epoch,DiversityRun.status==RunStatus.RUNNING).values(**values)).rowcount==1

def _fail(db,run_id,epoch,message):
 db.execute(update(DiversityRun).where(DiversityRun.id==run_id,DiversityRun.epoch==epoch,DiversityRun.status==RunStatus.RUNNING).values(status=RunStatus.FAILED,error=message[:1000]))
 db.commit();record(db,'worker','diversity.failed',{'run_id':run_id,'error':message[:1000]})

@app.task(name='ecology.analyze',soft_time_limit=180,time_limit=210)
def analyze_run(run_id,epoch=0):
 started=time.monotonic()
 try:
  with SessionLocal() as db:
   if not _claim(db,run_id,epoch):db.rollback();return
   db.commit();run=db.get(DiversityRun,run_id)
   try:prepared=prepare(row(db.get(Survey,run.survey_id)),[t for t in list_transects(db,run.survey_id) if t['id'] in set(run.config['transect_ids'])],run.config)
   except ValueError as e:db.rollback();_fail(db,run_id,epoch,str(e));return
   if run.params_fingerprint and run.params_fingerprint!=prepared['fingerprint']:
    _fail(db,run_id,epoch,'输入数据或参数已变化，已完成的样本不混用；请另建研究');return
   total=prepared['bootstrap']*prepared['eligible']
   if not _guarded(db,run_id,epoch,total_replicates=total,params_fingerprint=prepared['fingerprint']):db.rollback();return
   db.commit()
  B=prepared['bootstrap'];m=prepared['m'];seed=prepared['seed']
  for tid in prepared['order']:
   if tid not in prepared['vectors']:continue
   with SessionLocal() as db:done=sample_store.done_indices(db,run_id,tid)
   missing=[i for i in range(B) if i not in done]
   for offset in range(0,len(missing),settings.ANALYSIS_CHUNK_SIZE):
    if time.monotonic()-started>settings.ANALYSIS_CONTINUE_AFTER:enqueue_run(run_id,epoch);return
    rows=[]
    for i in missing[offset:offset+settings.ANALYSIS_CHUNK_SIZE]:
     if settings.ANALYSIS_STEP_DELAY:time.sleep(settings.ANALYSIS_STEP_DELAY)
     rows.append({'run_id':run_id,'transect_id':tid,**replicate_row(prepared['vectors'][tid],m,seed,prepared['keys'][tid],i)})
    with SessionLocal() as db:
     sample_store.insert_rows(db,rows)
     completed=select(func.count()).select_from(RunSample).where(RunSample.run_id==run_id).scalar_subquery()
     if not _guarded(db,run_id,epoch,completed_replicates=completed):db.rollback();return
     db.commit()
  with SessionLocal() as db:
   intervals={}
   for tid in prepared['vectors']:
    saved=sample_store.load(db,run_id,tid)
    if len(saved)!=B:enqueue_run(run_id,epoch);return
    intervals[tid]=intervals_from_rows(saved,B)
   result=assemble(prepared,intervals)
   Path(settings.ARTIFACT_DIR).mkdir(parents=True,exist_ok=True);Path(settings.ARTIFACT_DIR,f'diversity-{run_id}.json').write_text(json.dumps(result,ensure_ascii=False,allow_nan=False))
   if not _guarded(db,run_id,epoch,result=result,status=RunStatus.SUCCEEDED,error=None,completed_replicates=total):db.rollback();return
   db.commit();record(db,'worker','diversity.succeeded',{'run_id':run_id,'m':result['m'],'eligible':result['eligible'],'empty':result['excluded_empty']})
 except SoftTimeLimitExceeded:enqueue_run(run_id,epoch)
 except ValueError as e:
  with SessionLocal() as db:_fail(db,run_id,epoch,str(e))
 except Exception as e:
  # Unexpected interruption (process kill, transient storage failure): committed
  # replicates stay valid, the run remains resumable instead of restarting.
  log.exception('run %s interrupted',run_id)
  try:
   with SessionLocal() as db:record(db,'worker','diversity.interrupted',{'run_id':run_id,'epoch':epoch,'error':str(e)[:500]})
  except Exception:log.exception('run %s interruption audit failed',run_id)
