from sqlalchemy import select,update
from fastapi import HTTPException
from src.models import DiversityRun,Survey,Transect
from src.utils.serialize import row
from src.constants.enums import RunStatus
from .audit import record
from . import samples as sample_store
PAUSABLE=(RunStatus.QUEUED,RunStatus.RUNNING)
RESUMABLE=(RunStatus.PAUSED,RunStatus.RUNNING,RunStatus.QUEUED)
def list_runs(db):return [row(r) for r in db.scalars(select(DiversityRun).order_by(DiversityRun.id.desc()))]
def get_run(db,id):
 obj=db.get(DiversityRun,id)
 if not obj:raise HTTPException(404,'研究不存在')
 return obj
def run_detail(db,id):
 obj=get_run(db,id)
 return dict(row(obj),sample_progress=sample_store.counts_by_transect(db,id))
def create_run(db,data,user):
 if not db.get(Survey,data.survey_id):raise HTTPException(404,'调查期不存在')
 valid=set(db.scalars(select(Transect.id).where(Transect.survey_id==data.survey_id)))
 if not set(data.transect_ids)<=valid:raise HTTPException(422,'所选样线必须来自同一调查期')
 obj=DiversityRun(survey_id=data.survey_id,name=data.name,config=data.model_dump(exclude={'name','survey_id'}),creator_id=user.id);db.add(obj);db.commit()
 from src.worker import enqueue_run
 try:enqueue_run(obj.id,obj.epoch)
 except Exception:obj.status=RunStatus.FAILED;obj.error='队列不可用';db.commit();raise HTTPException(503,obj.error)
 record(db,user.email,'diversity.queued',{'run_id':obj.id});return row(obj)
def pause_run(db,id,user):
 get_run(db,id)
 # Confirming the pause bumps epoch in the same atomic write: any worker still
 # executing with the old epoch can no longer commit progress or publish results.
 claimed=db.execute(update(DiversityRun).where(DiversityRun.id==id,DiversityRun.status.in_(PAUSABLE)).values(status=RunStatus.PAUSED,epoch=DiversityRun.epoch+1)).rowcount
 if claimed!=1:raise HTTPException(409,'当前状态不可暂停')
 db.commit();obj=get_run(db,id);record(db,user.email,'diversity.paused',{'run_id':id,'epoch':obj.epoch});return row(obj)
def resume_run(db,id,user):
 get_run(db,id)
 # Atomic claim: concurrent resumes serialize here; only the latest epoch can
 # advance the study, older claimants are fenced out at their next write.
 claimed=db.execute(update(DiversityRun).where(DiversityRun.id==id,DiversityRun.status.in_(RESUMABLE)).values(status=RunStatus.RUNNING,epoch=DiversityRun.epoch+1)).rowcount
 if claimed!=1:raise HTTPException(409,'当前状态不可续算')
 db.commit();obj=get_run(db,id)
 from src.worker import enqueue_run
 try:enqueue_run(obj.id,obj.epoch)
 except Exception:raise HTTPException(503,'队列不可用，请重试续算')
 record(db,user.email,'diversity.resumed',{'run_id':id,'epoch':obj.epoch});return row(obj)
