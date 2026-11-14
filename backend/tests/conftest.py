import os,sys,tempfile,pathlib
TMP=tempfile.mkdtemp(prefix='ecology-resumable-')
os.environ['DATABASE_URL']=f"sqlite:///{TMP}/test.db?check_same_thread=false&timeout=30"
os.environ['JWT_SECRET']='test-secret'
os.environ['REDIS_URL']='redis://localhost:6399/0'
os.environ['ARTIFACT_DIR']=f'{TMP}/artifacts'
os.environ['SEED_DEMO']='false'
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import time,threading
from datetime import date
import pytest
from src.config.database import Base,engine,SessionLocal
from src.models import User,Survey,Transect,SpeciesCount,DiversityRun,RunSample,AuditLog,ConservationNote
from src.utils.security import hash_password,issue_token

Base.metadata.create_all(engine)

CONFIG={'m':10,'bootstrap':60,'seed':99}

def wait_until(cond,timeout=30,interval=0.05):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  if cond():return True
  time.sleep(interval)
 return False

@pytest.fixture()
def db():
 session=SessionLocal()
 try:yield session
 finally:
  session.rollback()
  for table in (RunSample,ConservationNote,DiversityRun,SpeciesCount,Transect,Survey,User,AuditLog):
   session.query(table).delete()
  session.commit();session.close()
  for f in pathlib.Path(os.environ['ARTIFACT_DIR']).glob('diversity-*.json'):f.unlink()

@pytest.fixture()
def study(db):
 for role in ('admin','analyst','viewer'):
  db.add(User(email=role+'@test.dev',name=role,role=role,password_hash=hash_password('Test2026!')))
 survey=Survey(name='测试调查期',survey_date=date(2026,10,14),filename='t.csv',description='')
 db.add(survey);db.flush()
 counts=[{'S01':40,'S02':25,'S03':15},{'S01':10,'S04':30,'S05':20},{}]
 ids=[]
 for i,vector in enumerate(counts):
  t=Transect(survey_id=survey.id,reference=f'T-{i+1:02d}',name=f'样线{i+1}',habitat='林缘');db.add(t);db.flush();ids.append(t.id)
  db.add_all([SpeciesCount(transect_id=t.id,species=s,count=c) for s,c in vector.items()])
 db.commit()
 return {'survey_id':survey.id,'transect_ids':ids}

@pytest.fixture()
def enqueued(monkeypatch):
 calls=[]
 import src.worker
 monkeypatch.setattr(src.worker,'enqueue_run',lambda run_id,epoch:calls.append((run_id,epoch)))
 return calls

@pytest.fixture()
def users(db,study):
 return {r:db.query(User).filter_by(email=r+'@test.dev').one() for r in ('admin','analyst','viewer')}

@pytest.fixture()
def client():
 from fastapi.testclient import TestClient
 from src.main import app
 with TestClient(app) as c:yield c

def token(user):return {'Authorization':'Bearer '+issue_token(user)}

def make_run(db,study,**overrides):
 config=dict(CONFIG,**overrides)
 creator=db.query(User).filter_by(role='admin').one()
 run=DiversityRun(survey_id=study['survey_id'],name='重采样研究',config={**config,'transect_ids':study['transect_ids']},creator_id=creator.id)
 db.add(run);db.commit();db.refresh(run)
 return run

def run_thread(run_id,epoch):
 t=threading.Thread(target=_drive,args=(run_id,epoch),daemon=True);t.start();return t

def _drive(run_id,epoch):
 from src.worker import analyze_run
 analyze_run(run_id,epoch)

def progress(db,run_id):
 run=db.get(DiversityRun,run_id);db.refresh(run)
 return run.status,run.completed_replicates,run.total_replicates

def saved(db,run_id):
 run=db.get(DiversityRun,run_id);db.refresh(run)
 return run

def sample_rows(db,run_id):
 return db.query(RunSample).filter_by(run_id=run_id).all()

def baseline(study):
 from src.services.analysis import analyze
 from src.services.transects import list_transects
 from src.utils.serialize import row
 from src.models import Survey
 with SessionLocal() as session:
  survey=row(session.get(Survey,study['survey_id']))
  data=[t for t in list_transects(session,study['survey_id']) if t['id'] in set(study['transect_ids'])]
 return analyze(survey,data,{**CONFIG,'transect_ids':study['transect_ids']})
