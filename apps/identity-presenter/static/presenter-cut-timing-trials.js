const configs=[
  {id:'center',number:'',layout:'center',name:'Candidate retrieval',title:'Turn messy guest records into a small set of useful comparisons.',copy:'Comparing every guest record with every other would take billions of comparisons. Embeddings let AI Search find the few worth making.'}
];
// Source rows are real records from the resolver release (release-evidence.js):
// the hero pair plus records from the sparse, corporate, household and
// default-DOB scenarios.
// Their vector coordinates are an illustrative teaching projection, not the
// 512-d embeddings.
const releaseRecords=Object.fromEntries(window.IDENTITY_RELEASE_EVIDENCE.scenarios.map(s=>[s.key,s]));
// The pipeline embeds each record's name and address separately (two 512-d vectors).
function releaseRow(record,v,cluster){
  // Illustrative 3-value previews of the two 512-d vectors; v is the plotted address vector.
  const nv=[v[1],-v[2],v[0]].map(x=>Math.round(x*100)/100);
  const address=[record.home_address_line1,record.home_suburb].filter(Boolean).join(', ')||'null';
  return {id:`${record.source_record_id.slice(0,8)}\u2026`,name:record.full_name,address,v,nv,cluster};
}
const rows=[
  releaseRow(releaseRecords['preferred-name'].left,[.36,.22,.61],1),
  releaseRow(releaseRecords.sparse.left,[-.48,.31,.14],0),
  releaseRow(releaseRecords.sparse.right,[-.44,.29,.11],0),
  releaseRow(releaseRecords.corporate.left,[.22,-.31,.24],2),
  releaseRow(releaseRecords.household.left,[.72,-.12,-.33],3)
];
const tailRow=releaseRow(releaseRecords['default-dob'].right,[.15,.48,.27],2);
const yutongPoint=releaseRow(releaseRecords['preferred-name'].right,[.52,.39,.59],1);
const releaseRecordCount=window.IDENTITY_RELEASE_EVIDENCE.headline.release_records.toLocaleString('en-AU');
const stages=[
  {title:'Encode',short:'Table → SQL → vectors',duration:5600},
  {title:'Place one point',short:'80% axes · 20% point, on every coordinate',duration:4200},
  {title:'Populate',short:'Deliberate ramp → rapid fill',duration:5000},
  {title:'Flatten',short:'Frozen 3D → readable 2D',duration:2000},
  {title:'Retrieve',short:'Candidate questions',duration:4400}
];
const narration=[
  ['Embed each record’s name and address.','Guest records arrive from Snowflake through Unity Catalog. The same model turns each record’s name and address into two separate 512-value vectors.'],
  ['Every record becomes a point.','Emily Zhang’s address vector is a position in space. The drawing shows three of its 512 dimensions; names get their own space and their own AI Search index.'],
  ['Similar records gather together.',`All ${releaseRecordCount} records are placed the same way. Records that read alike land in the same neighbourhood.`],
  ['Flatten to a readable map.','The same points, projected onto two dimensions so the neighbourhoods are easy to see. Distances are illustrative.'],
  ['Each neighbourhood raises a question.','AI Search returns each record’s nearest neighbours. Being close earns a comparison, not a match. The map is an illustrative 2-D projection.']
];
const state=Object.fromEntries(configs.map(c=>[c.id,{stage:0,progress:0,playing:false,all:false,start:0,timer:null,dirty:true}]));
const fmt=n=>`${n<0?'−':''}${Math.abs(n).toFixed(2).replace('0.','.')}`;
function typeLabel(kind,label){return `<span class="column-label"><i class="dtype dtype-${kind}">${kind==='number'?'1.2':'A<sup>B</sup>C'}</i><b>${label}</b></span>`}
function vectorCell(values,plotted){return `<code class="data-cell vector-cell"><span>[</span>${values.map((v,j)=>`<b${plotted?` data-value="${j}"`:''}>${fmt(v)}</b>`).join('<span>,</span>')}<span>, … ]</span></code>`}
function recordRow(r,rowIndex,indexLabel,output,extra=''){
  const cells=output
    ? `<span class="data-cell guest-cell">${r.name}</span>${vectorCell(r.nv,false)}${vectorCell(r.v,true)}`
    : `<span class="data-cell guest-cell">${r.name}</span><p class="data-cell semantic-cell">${r.address}</p>`;
  return `<div class="data-row${extra?' '+extra:''}" data-row="${rowIndex}"><i class="row-number">${indexLabel}</i>${cells}</div>`
}
function dataRows(output=false){return `<div class="column-row"><span class="row-number"></span>${output?typeLabel('text','guest')+typeLabel('number','name_embedding')+typeLabel('number','address_embedding'):typeLabel('text','name')+typeLabel('text','address')}</div>`+rows.map((r,i)=>recordRow(r,i,i+1,output)).join('')+`<div class="data-row ellipsis" data-row="5"><i class="row-number">⋮</i><span class="ellipsis-cell">⋮</span></div>`+recordRow(tailRow,6,releaseRecordCount,output,'tail-row')}
function table(output=false){return `<div class="table-card ${output?'vector-table':'source-table'}"><header><b>${output?'identity_search_records · 512-d vectors':'identity_search_records'}</b><span class="product-tag"><img src="assets/unity-catalog-icon-full-color.svg" alt="">Unity Catalog</span></header><div class="result-grid">${dataRows(output)}</div></div>`}
function sql(){return `<div class="sql-card"><span class="scan"></span><div class="sql-card-brand"><img src="assets/lakeflow-jobs-lockup-no-db-full-color.svg" alt="Lakeflow Jobs"></div><div class="sql-editor" aria-label="SQL embedding query"><div class="sql-line"><i>1</i><code><b>SELECT</b> record_key,</code></div><div class="sql-line"><i>2</i><code>&nbsp;&nbsp;<em>ai_query</em>(<span>'databricks-qwen3-embedding-0-6b'</span>,</code></div><div class="sql-line"><i>3</i><code>&nbsp;&nbsp;&nbsp;&nbsp;name) <b>AS</b> name_embedding,</code></div><div class="sql-line"><i>4</i><code>&nbsp;&nbsp;<em>ai_query</em>(<span>'databricks-qwen3-embedding-0-6b'</span>,</code></div><div class="sql-line"><i>5</i><code>&nbsp;&nbsp;&nbsp;&nbsp;address) <b>AS</b> address_embedding</code></div><div class="sql-line"><i>6</i><code><b>FROM</b> identity_search_records</code></div></div></div>`}
function scene(id){return `<div class="scene"><canvas data-canvas="${id}" aria-label="Animated vector placement and candidate neighbourhoods"></canvas><button class="replay" data-replay="${id}"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M13 8a5 5 0 1 1-1.5-3.6M13 2.5v3h-3" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>Replay</button><div class="ai-search-label"><img src="assets/ai-search-icon-full-color.svg" alt=""><b>AI Search</b></div></div>`}
function rail(id){return `<div class="stage-rail">${stages.map((s,i)=>`<button data-stage-button="${id}:${i}" aria-label="Play ${s.title}"><span class="stage-number">${i+1}</span><span class="stage-label">${s.title}<small>${s.short}</small></span><span class="stage-play">▶</span></button>`).join('')}</div>`}
function render(){document.getElementById('root').innerHTML=configs.map((c,i)=>`<section class="trial layout-${c.layout}${i===0?' active':''}" id="${c.id}" data-stage="0" data-encode-phase="source"><div class="heading"><div><h1>${c.title}</h1><p>${c.copy}</p></div><button class="play-all" data-play-all="${c.id}"><span>▶</span> Play full explanation</button></div><div class="demo">${scene(c.id)}<div class="encode-track">${table(false)}<span class="flow-arrow arrow-one">→</span>${sql()}<span class="flow-arrow arrow-two">→</span>${table(true)}</div></div>${rail(c.id)}</section>`).join('')}

function seeded(seed){let a=seed>>>0;return()=>{a|=0;a=a+0x6D2B79F5|0;let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296}}
const random=seeded(18092026),centres=[[-.4,.2,.14],[.34,.34,-.22],[.18,-.36,.3],[.58,-.14,-.3]];
const POINT_COUNT=150;
const points=Array.from({length:POINT_COUNT},(_,i)=>{if(i<rows.length)return{...rows[i],recordId:rows[i].id,id:i,x:rows[i].v[0],y:rows[i].v[1],z:rows[i].v[2]};if(i===rows.length){random();random();random();return{...yutongPoint,recordId:yutongPoint.id,id:i,x:yutongPoint.v[0],y:yutongPoint.v[1],z:yutongPoint.v[2]}}if(i===POINT_COUNT-1)return{...tailRow,recordId:tailRow.id,id:i,x:tailRow.v[0],y:tailRow.v[1],z:tailRow.v[2]};const cluster=i%4,c=centres[cluster];return{id:i,cluster,x:c[0]+(random()-.5)*.38,y:c[1]+(random()-.5)*.34,z:c[2]+(random()-.5)*.36}});
const canvasType=Object.assign({label:8,axis:8,value:8,hullHeight:22,hullBaseline:14,labelOffset:10},window.CANVAS_TYPE||{});
const palette=['#2272b4','#087f75','#7356a8','#a96107'],START_ANGLE=-.27,STOP_ANGLE=-.09;
const ease=t=>t<.5?2*t*t:1-Math.pow(-2*t+2,2)/2,clamp=t=>Math.max(0,Math.min(1,t)),lerp=(a,b,t)=>a+(b-a)*t;
function fit(canvas){const lw=canvas.clientWidth||1,lh=canvas.clientHeight||1,r=canvas.getBoundingClientRect(),visual=r.width?r.width/lw:1,ratio=Math.min((devicePixelRatio||1)*visual,3),width=Math.max(1,Math.round(lw*ratio)),height=Math.max(1,Math.round(lh*ratio));if(canvas.width!==width||canvas.height!==height){canvas.width=width;canvas.height=height}return{ctx:canvas.getContext('2d'),w:lw,h:lh,ratio}}
function worldPoint(world,w,h,angle){const origin={x:w*.39,y:h*.60},co=Math.cos(angle),si=Math.sin(angle),rx=world.x*co-world.z*si,rz=world.x*si+world.z*co,scale=Math.min(w,h)*.38;return{x:origin.x+rx*scale*1.4,y:origin.y-world.y*scale*1.2+rz*30}}
function anchor(w,h){return worldPoint(points[0],w,h,START_ANGLE)}
function projection(point,w,h,angle,flatten){const a=anchor(w,h),ref=points[0],dx=point.x-ref.x,dy=point.y-ref.y,dz=point.z-ref.z,co=Math.cos(angle),si=Math.sin(angle),rx=dx*co-dz*si,rz=dx*si+dz*co,scale=Math.min(w,h)*.47,perspective=1/(1.9+rz*.4);const x3=a.x+rx*scale*perspective*1.7,y3=a.y-dy*scale*perspective*1.5+rz*14;const cs=[[.39,.59],[.55,.36],[.69,.67],[.81,.43]],c=cs[point.cluster],base=centres[point.cluster];const within=(v,r)=>Math.max(-r,Math.min(r,v));let x2=c[0]*w+within(point.x-base[0],.19)*w*.25,y2=c[1]*h-within(point.y-base[1],.17)*h*.36;// The reference and its counterpart sit in the same vertical order as their table rows.
  if(point.id===0){x2=w*.505;y2=h*.3}if(point.id===rows.length){x2=w*.59;y2=h*.42}return{x:lerp(x3,x2,flatten),y:lerp(y3,y2,flatten),depth:rz}}
function axisPoint(world,w,h,angle){return worldPoint(world,w,h,angle)}
function drawAxes(ctx,w,h,angle,mode=3,alpha=1){const o=axisPoint({x:0,y:0,z:0},w,h,angle),x=axisPoint({x:.78,y:0,z:0},w,h,angle),y=axisPoint({x:0,y:.78,z:0},w,h,angle),z=axisPoint({x:0,y:0,z:.78},w,h,angle),ends=[x,y,z],labels=['x','y','z'];ctx.save();ctx.globalAlpha=alpha;ctx.lineWidth=1.25;ctx.font=`600 ${canvasType.axis}px Inter,sans-serif`;for(let i=0;i<mode;i++){ctx.strokeStyle=i===mode-1?'#5f6873':'#c1c8cf';ctx.beginPath();ctx.moveTo(o.x,o.y);ctx.lineTo(ends[i].x,ends[i].y);ctx.stroke();ctx.fillStyle='#5f6873';ctx.fillText(labels[i],ends[i].x+5,ends[i].y-4)}ctx.fillStyle='#5f6873';ctx.beginPath();ctx.arc(o.x,o.y,2.5,0,Math.PI*2);ctx.fill();ctx.fillText('0',o.x-10,o.y+11);ctx.restore()}
function drawTeachingAxes(ctx,w,h,p){
  const x=.36,y=.22,z=.61;
  const q1=ease(clamp(p/.27)),q2=ease(clamp((p-.27)/.27)),q3=ease(clamp((p-.54)/.27)),settle=ease(clamp((p-.81)/.19));
  const stage=p<.27?1:p<.54?2:3;
  const origin=axisPoint({x:0,y:0,z:0},w,h,START_ANGLE);
  const finalEnds=[axisPoint({x:.78,y:0,z:0},w,h,START_ANGLE),axisPoint({x:0,y:.78,z:0},w,h,START_ANGLE),axisPoint({x:0,y:0,z:.78},w,h,START_ANGLE)];
  const p1=axisPoint({x,y:0,z:0},w,h,START_ANGLE),p2=axisPoint({x,y,z:0},w,h,START_ANGLE),p3=anchor(w,h);
  const d1={x:p1.x-origin.x,y:p1.y-origin.y},d2={x:p2.x-p1.x,y:p2.y-p1.y},d3={x:p3.x-p2.x,y:p3.y-p2.y};
  const delta={x:d1.x*q1+d2.x*q2+d3.x*q3,y:d1.y*q1+d2.y*q2+d3.y*q3};
  const full={x:p3.x-origin.x,y:p3.y-origin.y};
  const axisOrigin={x:origin.x-.8*delta.x+.8*full.x*settle,y:origin.y-.8*delta.y+.8*full.y*settle};
  const point={x:origin.x+.2*delta.x+.8*full.x*settle,y:origin.y+.2*delta.y+.8*full.y*settle};
  const labels=['x','y','z'];
  const axisProgress=[q1,q2,q3];
  ctx.save();ctx.lineWidth=1.25;ctx.font=`600 ${canvasType.axis}px Inter,sans-serif`;
  for(let i=0;i<stage;i++){
    const reveal=axisProgress[i],end={x:axisOrigin.x+(finalEnds[i].x-origin.x)*reveal,y:axisOrigin.y+(finalEnds[i].y-origin.y)*reveal};
    ctx.globalAlpha=reveal;ctx.strokeStyle=i===stage-1?'#5d6670':'#aab1b8';
    ctx.beginPath();ctx.moveTo(axisOrigin.x,axisOrigin.y);ctx.lineTo(end.x,end.y);ctx.stroke();
    ctx.fillStyle='#5d6670';ctx.fillText(labels[i],end.x+6,end.y-4);
  }
  ctx.globalAlpha=1;ctx.fillStyle='#5d6670';ctx.beginPath();ctx.arc(axisOrigin.x,axisOrigin.y,2.4,0,Math.PI*2);ctx.fill();ctx.fillText('0',axisOrigin.x-10,axisOrigin.y+12);ctx.restore();
  return point;
}
function placement(ctx,w,h,p){const point=drawTeachingAxes(ctx,w,h,p);ctx.fillStyle='#ff3621';ctx.beginPath();ctx.arc(point.x,point.y,6,0,Math.PI*2);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.fillStyle='#111827';ctx.font=`700 ${canvasType.label}px Inter,sans-serif`;ctx.fillText('Emily Zhang',point.x+canvasType.labelOffset,point.y+canvasType.label*.36);if(p>.67){ctx.globalAlpha=ease(clamp((p-.67)/.22));ctx.font=`600 ${canvasType.value}px Inter,sans-serif`;ctx.fillStyle='#5f6873';ctx.fillText('[ .36, .22, .61 ]',point.x+canvasType.labelOffset,point.y-canvasType.value*1.6);ctx.globalAlpha=1}}
function pointBirth(i,p){if(i===0)return 1;if(i<5){const start=.08+(i-1)*.115;return ease(clamp((p-start)/.075))}if(i===points.length-1)return ease(clamp((p-.90)/.07));const t=(i-5)/(points.length-6),start=.58+Math.pow(t,1.45)*.29;return ease(clamp((p-start)/.04))}
function hull(ctx,x,y,rx,ry,color,label,delay,p,below=false){const q=ease(clamp((p-delay)/.23));ctx.save();ctx.globalAlpha=q;ctx.fillStyle=color+'10';ctx.strokeStyle=color+'5f';ctx.setLineDash([4,4]);ctx.beginPath();ctx.ellipse(x,y,rx,ry,-.08,0,Math.PI*2);ctx.fill();ctx.stroke();ctx.setLineDash([]);ctx.font=`700 ${canvasType.label}px Inter,sans-serif`;const width=ctx.measureText(label).width+16,labelX=x-width/2,labelY=below?y+ry+6:y-ry-6-canvasType.hullHeight;ctx.fillStyle='#fff';ctx.strokeStyle=color+'8f';ctx.beginPath();ctx.roundRect(labelX,labelY,width,canvasType.hullHeight,4);ctx.fill();ctx.stroke();ctx.fillStyle=color;ctx.fillText(label,labelX+8,labelY+canvasType.hullBaseline);ctx.restore()}
const questions=[`Is ${releaseRecords.sparse.right.full_name} the same as ${releaseRecords.sparse.left.full_name}?`,`Is ${releaseRecords['preferred-name'].right.full_name} the same as ${releaseRecords['preferred-name'].left.full_name}?`,'Do shared Pacific Events contacts identify one person?','Does a shared home phone mean the same guest?'];
// Retrieve (stage 4) flattens the frozen 3D frame first, then the neighbourhoods appear.
const RETRIEVE_FLATTEN=.42;
function universe(ctx,w,h,stage,p){const flatten=stage===3?ease(p):stage===4?ease(clamp(p/RETRIEVE_FLATTEN)):0,angle=stage===2?lerp(START_ANGLE,STOP_ANGLE,ease(clamp((p-.16)/.68))):STOP_ANGLE;drawAxes(ctx,w,h,angle,3,1-flatten);const projected=points.map(q=>({...q,...projection(q,w,h,angle,flatten),birth:stage===2?pointBirth(q.id,p):1})).sort((a,b)=>a.depth-b.depth);if(stage===4){const hp=clamp((p-RETRIEVE_FLATTEN)/(1-RETRIEVE_FLATTEN));hull(ctx,w*.39,h*.59,w*.08,h*.12,palette[0],questions[0],.02,hp,true);hull(ctx,w*.55,h*.375,w*.085,h*.13,palette[1],questions[1],.18,hp);hull(ctx,w*.69,h*.67,w*.08,h*.12,palette[2],questions[2],.34,hp,true);hull(ctx,w*.81,h*.42,w*.08,h*.12,palette[3],questions[3],.5,hp)}projected.forEach(q=>{if(q.birth<=0||q.id===0||(stage===4&&p>=RETRIEVE_FLATTEN&&q.id===rows.length))return;ctx.globalAlpha=q.birth*(stage===4&&p>=RETRIEVE_FLATTEN?.82:.68);ctx.fillStyle=stage===4&&p>=RETRIEVE_FLATTEN?palette[q.cluster]:'#89949e';ctx.beginPath();ctx.arc(q.x,q.y,3.2*(.3+.7*q.birth),0,Math.PI*2);ctx.fill()});ctx.globalAlpha=1;const ref=projected.find(q=>q.id===0);ctx.fillStyle='#ff3621';ctx.beginPath();ctx.arc(ref.x,ref.y,6,0,Math.PI*2);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.fillStyle='#111827';ctx.font=`700 ${canvasType.label}px Inter,sans-serif`;ctx.fillText('Emily Zhang',ref.x+canvasType.labelOffset,ref.y+canvasType.label*.36);if(stage===4&&p>=RETRIEVE_FLATTEN){const counterpart=projected.find(q=>q.id===rows.length);ctx.fillStyle=palette[1];ctx.beginPath();ctx.arc(counterpart.x,counterpart.y,6,0,Math.PI*2);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.fillStyle='#111827';ctx.fillText('张雨桐',counterpart.x+canvasType.labelOffset,counterpart.y+canvasType.label*.36)}}
function draw(id){const s=state[id],canvas=document.querySelector(`canvas[data-canvas="${id}"]`);if(!canvas||!canvas.offsetParent)return;const{ctx,w,h,ratio}=fit(canvas);ctx.save();ctx.scale(ratio,ratio);ctx.clearRect(0,0,w,h);if(s.stage===1)placement(ctx,w,h,s.progress);else if(s.stage>1)universe(ctx,w,h,s.stage,s.progress);ctx.restore()}
function encodePhase(s){if(s.stage!==0)return'vector';if(!s.playing&&s.progress===0)return'source';return s.progress<.035?'intro':s.progress<.34?'source':s.progress<.67?'sql':'vector'}
function activeRow(s){if(s.stage===1)return 0;if(s.stage===2){if(s.progress<.58)return Math.min(4,Math.floor(s.progress/.116));if(s.progress<.90)return 5;return 6}return-1}
function activeValue(s){if(s.stage!==1)return -1;return s.progress<.27?0:s.progress<.54?1:2}
function update(id){const s=state[id];s.dirty=true;const el=document.getElementById(id),row=activeRow(s),value=activeValue(s);el.dataset.stage=s.stage;el.dataset.encodePhase=encodePhase(s);el.classList.toggle('is-playing',s.playing);el.querySelectorAll('.vector-table .data-row').forEach(r=>r.classList.toggle('active',Number(r.dataset.row)===row));el.querySelectorAll('.vector-table [data-value]').forEach(v=>v.classList.toggle('active',Number(v.dataset.value)===value&&Number(v.closest('.data-row').dataset.row)===row));el.querySelectorAll('button[data-stage-button]').forEach((b,i)=>{b.classList.toggle('active',i===s.stage);b.classList.toggle('done',i<s.stage);b.classList.toggle('playing',i===s.stage&&s.playing)})}
function stop(id){const s=state[id];clearTimeout(s.timer);s.playing=false;s.all=false;const b=document.querySelector(`[data-play-all="${id}"]`);if(b){b.classList.remove('playing');b.innerHTML='<span>▶</span> Play full explanation'}update(id)}
function play(id,stage,all=false){const s=state[id];clearTimeout(s.timer);s.stage=stage;s.progress=0;s.start=performance.now();s.playing=true;s.all=all;update(id);if(all)s.timer=setTimeout(()=>stage<4?play(id,stage+1,true):stop(id),stages[stage].duration+260)}
function playAll(id){configs.forEach(c=>{if(c.id!==id)stop(c.id)});const b=document.querySelector(`[data-play-all="${id}"]`);b.classList.add('playing');b.innerHTML='<span>▶</span> Playing…';play(id,0,true)}
function show(id){configs.forEach(c=>{document.getElementById(c.id).classList.toggle('active',c.id===id);if(c.id!==id)stop(c.id)});document.querySelectorAll('[data-trial]').forEach(b=>b.classList.toggle('active',b.dataset.trial===id))}
render();configs.forEach(c=>update(c.id));
document.querySelectorAll('[data-trial]').forEach(b=>b.addEventListener('click',()=>show(b.dataset.trial)));
document.querySelectorAll('[data-play-all]').forEach(b=>b.addEventListener('click',()=>playAll(b.dataset.playAll)));
document.querySelectorAll('button[data-stage-button]').forEach(b=>b.addEventListener('click',()=>{const[id,stage]=b.dataset.stageButton.split(':');play(id,Number(stage),false)}));
document.querySelectorAll('[data-replay]').forEach(b=>b.addEventListener('click',()=>play(b.dataset.replay,3,false)));
document.addEventListener('keydown',e=>{if(!['ArrowLeft','ArrowRight'].includes(e.key))return;const ids=configs.map(c=>c.id),active=document.querySelector('.trial.active').id,index=ids.indexOf(active);show(ids[(index+(e.key==='ArrowRight'?1:-1)+ids.length)%ids.length])});
function frame(now){configs.forEach(c=>{const s=state[c.id];if(s.playing){s.progress=Math.min(1,(now-s.start)/stages[s.stage].duration);if(s.progress>=1&&!s.all)s.playing=false;update(c.id)}if(s.dirty){s.dirty=false;draw(c.id)}});requestAnimationFrame(frame)}
window.addEventListener('resize',()=>configs.forEach(c=>{state[c.id].dirty=true}));requestAnimationFrame(frame);
