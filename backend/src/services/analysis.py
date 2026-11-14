import numpy as np
from src.constants.species import SPECIES
from .rarefaction import valid_counts,expected_species
from .diversity import indices
from .bootstrap import replicate_row,intervals_from_rows
from .fingerprint import fingerprint

DEFINITIONS={'rarefaction':'individual-based without replacement E[S_m]=sum(1-C(N-n_i,m)/C(N,m)); same survey, same m','shannon':'-sum(p_i ln p_i), plug-in at original N','simpson':'1-sum(p_i^2), plug-in at original N','intervals':'conditional observed-composition multinomial percentile95%; not full field-sampling uncertainty'}
INTERPRETATION='Compare rarefied richness only at common m; do not rank raw richness or original-N indices as if effort were equal; observational differences do not establish conservation causality.'

def prepare(survey,transects,config):
 # Validates every input up front so a run fails before any replicate is persisted.
 if not transects:raise ValueError('未选择样线')
 if len({t['id'] for t in transects})!=len(transects) or len({t['reference'] for t in transects})!=len(transects):raise ValueError('样线重复')
 if any(t['survey_id']!=survey['id'] for t in transects):raise ValueError('只能比较同一调查期样线')
 if set(config['transect_ids'])!={t['id'] for t in transects}:raise ValueError('选择样线与配置不一致')
 vectors={}
 for t in transects:
  if not set(t['counts'])<=set(SPECIES):raise ValueError('未知物种')
  vectors[t['id']]=valid_counts([t['counts'].get(s,0) for s in SPECIES])
 eligible=[t for t in transects if vectors[t['id']].sum()>0]
 if not eligible:raise ValueError('所有样线为空，无可比较群落')
 common_limit=min(int(vectors[t['id']].sum()) for t in eligible);m=config['m']
 if not isinstance(m,int) or not 0<=m<=common_limit:raise ValueError(f'共同稀释量m超出资格范围0..{common_limit}')
 grid=sorted(set(np.linspace(0,common_limit,41,dtype=int).tolist()+[m]));static={};order=[]
 for t in sorted(transects,key=lambda x:x['reference']):
  a=vectors[t['id']];N=int(a.sum());meta={k:t[k] for k in ('id','reference','name','habitat')};order.append(t['id'])
  if not N:
   static[t['id']]={**meta,'status':'empty','n':0,'observed_species':0,'rarefied':None,'shannon':None,'simpson':None,'curve':[],'intervals':None,'reason':'空样线单列；不参加共同m比较'};continue
  result=indices(a);result.update(meta,status='eligible',n=N,rarefied=expected_species(a,m),curve=[{'m':x,'expected':expected_species(a,x)} for x in grid],rare_species=[{'species':s,'count':int(c)} for s,c in zip(SPECIES,a) if 0<c<=2]);static[t['id']]=result
 return {'fingerprint':fingerprint(survey,transects,config),'survey_id':survey['id'],'m':m,'common_limit':common_limit,'eligible':len(eligible),'excluded_empty':len(transects)-len(eligible),'bootstrap':config['bootstrap'],'seed':config['seed'],'order':order,'static':static,'vectors':{t['id']:vectors[t['id']] for t in eligible},'keys':{t['id']:str(survey['id'])+'/'+t['reference'] for t in eligible}}

def assemble(prepared,intervals_by_tid):
 transects=[]
 for tid in prepared['order']:
  entry=dict(prepared['static'][tid])
  if entry['status']=='eligible':entry['intervals']=intervals_by_tid[tid]
  transects.append(entry)
 return {k:prepared[k] for k in ('fingerprint','m','common_limit','eligible','excluded_empty','bootstrap','seed')}|{'transects':transects,'definitions':DEFINITIONS,'interpretation':INTERPRETATION}

def analyze(survey,transects,config):
 prepared=prepare(survey,transects,config)
 intervals={tid:intervals_from_rows([replicate_row(prepared['vectors'][tid],prepared['m'],prepared['seed'],prepared['keys'][tid],i) for i in range(prepared['bootstrap'])],prepared['bootstrap']) for tid in prepared['vectors']}
 return assemble(prepared,intervals)
