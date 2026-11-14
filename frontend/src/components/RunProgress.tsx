import type {Run} from '../types/domain';
export function RunProgress({run}:{run:Run}){
 if(run.status==='failed')return <small className="progress-note">—</small>;
 if(!run.total_replicates)return <small className="progress-note">等待worker分配</small>;
 const pct=Math.min(100,Math.round(100*run.completed_replicates/run.total_replicates));
 return <div className="progress"><div className="track"><div className="fill" style={{width:pct+'%'}}/></div><small>{run.completed_replicates}/{run.total_replicates} · {pct}%</small></div>;
}
