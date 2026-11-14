import hashlib
import numpy as np
from .diversity import indices
from .rarefaction import valid_counts,expected_species

METHOD='percentile 95%; multinomial(N, observed p), conditional on observed composition; unseen species/spatial dependence/detection uncertainty excluded'

def check_params(counts,repetitions,seed):
 a=valid_counts(counts)
 if int(a.sum())==0:raise ValueError('空群落不可bootstrap')
 if not isinstance(repetitions,int) or not 50<=repetitions<=1000:raise ValueError('bootstrap次数需50..1000')
 if not isinstance(seed,int) or not 0<=seed<=2147483647:raise ValueError('seed范围不合法')
 return a

def key_seed(key):return int.from_bytes(hashlib.sha256(key.encode()).digest()[:4],'big')

def replicate_sample(a,seed,key,index):
 # Per-replicate deterministic stream: the same (seed,key,index) yields the same
 # sample no matter how the replicates are chunked across pause/resume sessions.
 rng=np.random.default_rng(np.random.SeedSequence([seed,key_seed(key),index]))
 return rng.multinomial(int(a.sum()),a/a.sum())

def replicate_row(counts,m,seed,key,index):
 a=valid_counts(counts);sample=replicate_sample(a,seed,key,index);result=indices(sample)
 return {'replicate_index':int(index),'shannon':result['shannon'],'simpson':result['simpson'],'rarefied':expected_species(sample,m)}

def intervals_from_rows(rows,repetitions):
 H=[r['shannon'] for r in rows];D=[r['simpson'] for r in rows];R=[r['rarefied'] for r in rows]
 def interval(x):return np.quantile(x,[.025,.975]).tolist()
 return {'shannon_ci':interval(H),'simpson_ci':interval(D),'rarefied_ci':interval(R),'repetitions':repetitions,'method':METHOD}

def bootstrap_intervals(counts,m,repetitions,seed,key):
 a=check_params(counts,repetitions,seed)
 expected_species(a,m)
 return intervals_from_rows([replicate_row(a,m,seed,key,i) for i in range(repetitions)],repetitions)
