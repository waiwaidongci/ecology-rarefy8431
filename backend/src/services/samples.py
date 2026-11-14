from sqlalchemy import select,func
from src.models import RunSample

def insert_rows(db,rows):
 # Completed replicates are keyed by (run_id,transect_id,replicate_index); a
 # duplicate insert from a redelivered or overlapping worker is skipped, never
 # double-counted.
 if not rows:return
 dialect=db.get_bind().dialect.name
 if dialect=='postgresql':
  from sqlalchemy.dialects.postgresql import insert
  db.execute(insert(RunSample).values(rows).on_conflict_do_nothing())
 elif dialect=='sqlite':
  from sqlalchemy.dialects.sqlite import insert
  db.execute(insert(RunSample).values(rows).on_conflict_do_nothing())
 else:db.execute(RunSample.__table__.insert().values(rows))

def done_indices(db,run_id,transect_id):
 return set(db.scalars(select(RunSample.replicate_index).where(RunSample.run_id==run_id,RunSample.transect_id==transect_id)))

def count(db,run_id):
 return db.scalar(select(func.count()).select_from(RunSample).where(RunSample.run_id==run_id)) or 0

def counts_by_transect(db,run_id):
 return {tid:done for tid,done in db.execute(select(RunSample.transect_id,func.count()).where(RunSample.run_id==run_id).group_by(RunSample.transect_id))}

def load(db,run_id,transect_id):
 return [dict(replicate_index=s.replicate_index,shannon=s.shannon,simpson=s.simpson,rarefied=s.rarefied) for s in db.scalars(select(RunSample).where(RunSample.run_id==run_id,RunSample.transect_id==transect_id).order_by(RunSample.replicate_index))]
