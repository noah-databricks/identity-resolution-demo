// Nine-stage presenter. `duration` is the animation length of each stage.
const ixStageDefinitions=[
  {title:'Encode',short:'To vectors',duration:5600},
  {title:'Place',short:'One vector',duration:4200},
  {title:'Populate',short:'Vector universe',duration:5000},
  {title:'Retrieve',short:'Top 20',duration:4400},
  {title:'Select pair',short:'One pair',duration:1900},
  {title:'Reveal rows',short:'Source rows',duration:1700},
  {title:'Compare',short:'Field weights',duration:2000},
  {title:'Resolve',short:'Match policy',duration:7800},
  {title:'Hand off',short:'To stewards',duration:5600}
];

// Rail stages 0-3 play the base trial's canvas stages; Retrieve (rail 3) is trial stage 4,
// which flattens 3D to 2D before the neighbourhoods appear. Rail 4-8 are the matching stages.
const IX_TRIAL_STAGE=[0,1,2,4];
const IX_SELECT=4,IX_REVEAL=5,IX_COMPARE=6,IX_RESOLVE=7,IX_HANDOFF=8;
// Overlay match step for a matching stage: Select pair is step 1 ... Hand off is step 5.
const ixMatchStep=stage=>String(stage-IX_SELECT+1);
const ixScenarios=window.IDENTITY_SCENARIOS;
const ixRelease=window.IDENTITY_RELEASE_EVIDENCE;
const ixHandoff=ixRelease.handoff;
// Hand-off graph node positions in a 600x400 box: A C D form the customer, B is held.
const IX_NODE_POS=[[110,70],[110,330],[450,70],[450,215]];
const ixHero=ixScenarios[0];
const ixReducedMotion=window.matchMedia('(prefers-reduced-motion: reduce)');
const ixReduced=()=>ixReducedMotion.matches;

const ixTrial=document.getElementById('center');
const ixDemo=ixTrial.querySelector('.demo');
// Stage navigation: chevrons either side of the viewer (the stage rail is removed).
ixTrial.querySelector('.stage-rail').remove();
const ixShell=document.createElement('div');ixShell.className='ix-stage-shell';ixDemo.before(ixShell);
const ixChevron=(dir,label,d)=>{const b=document.createElement('button');b.className=`ix-nav ix-nav-${dir}`;b.setAttribute('aria-label',label);b.innerHTML=`<svg viewBox="0 0 24 24" aria-hidden="true"><path d="${d}"/></svg>`;return b};
const ixPrev=ixChevron('prev','Previous stage','M15 4.5 7.5 12l7.5 7.5'),ixNext=ixChevron('next','Next stage','M9 4.5 16.5 12 9 19.5');
ixShell.append(ixPrev,ixDemo,ixNext);
let ixTimers=[];
let ixActiveStage=-1;
let ixActiveScenario=0;
let ixEvidenceToken=0;

// The presenter is laid out on one fixed logical frame and scaled uniformly to
// the screen, so 1280x720 laptops and 1920x1080 projectors show the same
// composition with no clipping and proportionally larger type.
const IX_FRAME={width:1520,height:800};
function ixFitFrame(){
  const scale=Math.min(window.innerWidth/IX_FRAME.width,window.innerHeight/IX_FRAME.height),root=document.documentElement;
  root.style.setProperty('--ix-k',String(scale));
  root.style.setProperty('--ix-frame-w',`${window.innerWidth/scale}px`);
  root.style.setProperty('--ix-frame-h',`${window.innerHeight/scale}px`);
  document.body.classList.add('ix-framed');
}
ixFitFrame();
// Converts getBoundingClientRect (screen pixels) into the frame's layout pixels.
function ixVisualScale(){const width=ixDemo.offsetWidth;return width?ixDemo.getBoundingClientRect().width/width:1}

const ixHullGeometries=[
  {className:'ix-name',cx:.39,cy:.59,rx:.08,ry:.12,label:questions[0]},
  {className:'ix-selected',cx:.55,cy:.375,rx:.085,ry:.13,label:questions[1]},
  {className:'ix-corporate',cx:.69,cy:.67,rx:.08,ry:.12,label:questions[2]},
  {className:'ix-household',cx:.81,cy:.42,rx:.08,ry:.12,label:'Does a shared home phone mean the same guest?'}
];

function ixPointMarkup(point){
  if(point.id===0)return `<div class="ix-point ix-left" data-ix-source-point="0" data-ix-point="left"><i></i><span>${ixHero.left.values[0]}</span></div>`;
  if(point.id===rows.length)return `<div class="ix-point ix-right" data-ix-source-point="${point.id}" data-ix-point="right"><i></i><span>${ixHero.right.values[0]}</span></div>`;
  return `<i class="ix-ambient" data-ix-source-point="${point.id}"></i>`;
}

function ixClusterMarkup(cluster){
  const geometry=ixHullGeometries[cluster];
  return `<div class="ix-cluster ${geometry.className}" data-ix-cluster="${cluster}">${points.filter(point=>point.cluster===cluster).map(ixPointMarkup).join('')}<strong>${geometry.label}</strong></div>`;
}

function ixSyntax(sql){return sql.replace(/^([a-z_][a-z0-9_]*)(?=\()/,'<em>$1</em>').replace(/\b(AS|AND|NOT|OR)\b/g,'<b>$1</b>')}
function ixColumnName(field){return field.column||field.label.toLowerCase().replace(/\s+/g,'_')}
function ixEscape(value){return String(value).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'})[c])}
function ixCellValue(value){return value===null||value===undefined?'<i class="ix-null">null</i>':ixEscape(value)}
function ixRecordCells(record,tag){return `<b>${ixEscape(record.source)}</b><code title="${ixEscape(record.recordId)}">${ixEscape(record.id)}</code>${record.values.map((value,index)=>`<${tag} data-ix-col="${index}">${ixCellValue(value)}</${tag}>`).join('')}`}
function ixFormatCount(n){return Number(n).toLocaleString('en-AU')}
function ixEvidenceMarkup(){return `<div class="ix-evidence-view" data-ix-evidence-view>
  <nav class="ix-scenario-picker" aria-label="Matching scenarios">${ixScenarios.map((scenario,index)=>`<button data-ix-scenario="${index}" class="${index===ixActiveScenario?'active':''}" aria-pressed="${index===ixActiveScenario}"><i>${index+1}</i><span>${scenario.short}</span></button>`).join('')}</nav>
  <div class="ix-evidence-heading"><div><b data-ix-evidence-title></b><span data-ix-route-detail></span></div></div>
  <div class="ix-evidence-layout">
    <section class="ix-ledger" data-ix-ledger>
      <header><b>Field by field</b><span data-ix-progress></span></header>
      <div class="ix-ledger-head" data-ix-ledger-head></div>
      <div class="ix-ledger-rows" data-ix-evidence-items></div>
      <div class="ix-ledger-total" data-ix-total></div>
      <details class="ix-sql-note"><summary><img src="assets/lakeflow-jobs-lockup-no-db-full-color.svg" alt="Lakeflow Jobs"><span>Weights computed by Splink in the pipeline job</span><u>Show SQL</u></summary><pre data-ix-sql></pre></details>
    </section>
    <aside class="ix-decision" data-ix-verdict aria-live="polite">
      <header><b>decide_v4</b><span>Versioned match policy p4</span></header>
      <div class="ix-decision-body" data-ix-decision-body></div>
      <footer class="ix-destination" data-ix-destination></footer>
    </aside>
  </div>
</div>`}

function ixOverlayMarkup(){return `<div class="ix-overlay" data-match-step="0">
  <div class="ix-universe">
    <div class="ix-universe-label"><img src="assets/ai-search-icon-full-color.svg" alt=""><b>AI Search</b></div>
    ${ixHullGeometries.map((_,cluster)=>ixClusterMarkup(cluster)).join('')}
  </div>
  <svg class="ix-tethers" aria-hidden="true"><path data-ix-tether="left" pathLength="1"/><path data-ix-tether="right" pathLength="1"/><path class="ix-arrow" data-ix-arrow="left"/><path class="ix-arrow" data-ix-arrow="right"/></svg>
  <div class="ix-flow-table">
    <header><b>selected_candidate_records</b></header>
    <div class="ix-flow-head"><span>source</span><span>record_id</span>${ixHero.fields.map(field=>`<span>${ixColumnName(field)}</span>`).join('')}</div>
    <div class="ix-flow-row ix-left-row" data-ix-row="left"><i class="ix-row-anchor"></i>${ixRecordCells(ixHero.left,'span')}</div>
    <div class="ix-flow-row ix-right-row" data-ix-row="right"><i class="ix-row-anchor"></i>${ixRecordCells(ixHero.right,'span')}</div>
  </div>
  ${ixEvidenceMarkup()}
</div>`}

ixDemo.insertAdjacentHTML('beforeend',ixOverlayMarkup());
const ixOverlay=ixDemo.querySelector('.ix-overlay');
ixOverlay.insertAdjacentHTML('beforeend',ixHandoffMarkup());
const ixEvidenceView=ixOverlay.querySelector('[data-ix-evidence-view]');

// Compare and Resolve share one ledger: a row per Splink comparison with both records'
// values, a plain result and a weight bar from a centre line. Resolve reveals the rows,
// then the match policy's decision.
const IX_WEIGHT_SCALE=Math.max(...ixScenarios.flatMap(s=>s.fields.map(f=>Math.abs(f.weight))),1);
const IX_RESULT={Exact:'Match',Disagree:'Different',Missing:'Missing'};
function ixResult(field){
  if(field.kind==='reject')return {text:'Conflict',tone:'neg'};
  if(field.kind==='ignored')return {text:'Not used',tone:'muted'};
  if(field.kind==='insufficient')return {text:'Missing',tone:'muted'};
  if(IX_RESULT[field.level])return {text:IX_RESULT[field.level],tone:field.weight>0?'pos':'neg'};
  return {text:/0\.9/.test(field.level)?'Close':'Similar',tone:field.weight>0?'pos':'neg'};
}
function ixValue(value){return value===null||value===undefined||value===''?'<i class="ix-null">not recorded</i>':ixEscape(value)}
function ixBar(weight){
  const pct=Math.min(50,Math.abs(weight)/IX_WEIGHT_SCALE*50).toFixed(2),side=weight>=0?'pos':'neg';
  return `<span class="ix-track"><i class="ix-bar ix-bar-${side}" style="--w:${pct}%"></i></span><b class="ix-w ix-w-${Math.abs(weight)<.005?'zero':side}">${formatWeight(weight)}</b>`;
}
// The policy clause that fired, as plain conditions (resolution/splink_v4/policy.py).
function ixPolicyChecks(scenario){
  const exact=col=>scenario.fields.some(f=>f.splinkColumn===col&&f.level==='Exact');
  const contact=exact('personal_phone_n')?'Same personal phone':'Same personal email';
  return ({
    PERSONAL_CONTACT_AND_DOB:[[contact,true],['Birth dates match',true]],
    BOTH_PERSONAL_CONTACTS:[['Same personal email',true],['Same personal phone',true],['Names do not conflict',true]],
    PERSONAL_CONTACT_AND_NAME:[[contact,true],['Names agree',true]],
    VALID_DOB_CONFLICT:[['Two valid birth dates differ',false]],
    INSUFFICIENT_EVIDENCE_REVIEW:[[`Model is confident (${scenario.probabilityLabel})`,true],['Independent personal evidence: email, phone or birth date',false]],
    LOW_MATCH_PROBABILITY:[['Model probability high enough to link',false]]
  })[scenario.decisionRule]||[[scenario.outcome,true]];
}
const IX_OUTCOME={'auto-match':'Linked as one guest','reject':'Rejected','separate':'Kept separate','review':'Sent to a steward'};
const ixIcon=ok=>ok?'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8.5l3 3 6-7"/></svg>':'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4.5 4.5l7 7M11.5 4.5l-7 7"/></svg>';
function ixRenderEvidence(){
  const scenario=ixScenarios[ixActiveScenario],view=ixEvidenceView,rec=r=>`<span><b>${ixEscape(r.source)}</b></span>`;
  view.querySelector('[data-ix-evidence-title]').textContent=scenario.title;
  view.querySelector('[data-ix-route-detail]').textContent=scenario.routeDetail;
  view.querySelector('[data-ix-ledger-head]').innerHTML=`<span>Field</span>${rec(scenario.left)}${rec(scenario.right)}<span>Result</span><span class="ix-weight ix-weight-head"><span class="ix-track-labels"><i>Against a match</i><i>For a match</i></span><b class="ix-w"></b></span>`;
  const rows=scenario.fields.map((field,i)=>{const r=ixResult(field);return `<div class="ix-row" data-ix-evidence-item="${i}"><span class="ix-field">${ixEscape(field.label)}</span><span>${ixValue(scenario.left.values[i])}</span><span>${ixValue(scenario.right.values[i])}</span><span class="ix-result ix-tone-${r.tone}">${r.text}</span><span class="ix-weight">${ixBar(field.weight)}</span>${field.emphasis?`<small class="ix-row-note">${ixEscape(field.detail)}</small>`:''}</div>`}).join('');
  const context=scenario.context.length?`<div class="ix-context-label">Also shared, but not scored by Splink</div>${scenario.context.map((name,i)=>`<div class="ix-row ix-row-context"><span class="ix-field">${ixEscape(name.replace(/_/g,' '))}</span><span>${ixValue(scenario.left.context[i])}</span><span>${ixValue(scenario.right.context[i])}</span><span class="ix-result ix-tone-muted">Candidate only</span><span></span></div>`).join('')}`:'';
  view.querySelector('[data-ix-evidence-items]').innerHTML=rows+context;
  view.querySelector('[data-ix-sql]').innerHTML=scenario.fields.map((f,i)=>`${i===0?'<b>SELECT</b> ':'       '}${ixSyntax(f.sql)}`).join('\n');
  view.querySelectorAll('[data-ix-scenario]').forEach((button,index)=>{button.classList.toggle('active',index===ixActiveScenario);button.setAttribute('aria-pressed',String(index===ixActiveScenario))});
  view.querySelector('[data-ix-progress]').textContent=`0 of ${scenario.fields.length} evaluated`;
  ixRenderTotal(scenario,-1);
  const panel=view.querySelector('[data-ix-verdict]');panel.className='ix-decision';
  view.querySelector('[data-ix-decision-body]').innerHTML=`<p class="ix-pending">The policy decides once all ${scenario.fields.length} comparisons are in.</p><strong class="ix-decision-code" data-ix-decision></strong>`;
  view.querySelector('[data-ix-destination]').innerHTML='';
}
function ixRenderTotal(scenario,index){
  const weight=scenario.priorWeight+scenario.fields.slice(0,index+1).reduce((t,f)=>t+f.weight,0),done=index===scenario.fields.length-1;
  ixEvidenceView.querySelector('[data-ix-total]').innerHTML=`<span>Total</span><span class="ix-total-note">Starts at ${formatWeight(scenario.priorWeight)}: any two records are unlikely to match</span><b class="ix-total-weight">${formatWeight(weight)}</b><span class="ix-total-prob${done?' done':''}">Splink probability <b>${done?scenario.probabilityLabel:'…'}</b></span>`;
}
function ixRevealEvidence(index,token){
  if(token!==ixEvidenceToken)return;const scenario=ixScenarios[ixActiveScenario],view=ixEvidenceView;
  view.querySelectorAll('[data-ix-evidence-item]').forEach((row,i)=>{row.classList.toggle('revealed',i<=index);row.classList.toggle('active',i===index)});
  view.querySelector('[data-ix-progress]').textContent=`${index+1} of ${scenario.fields.length} evaluated`;
  ixRenderTotal(scenario,index);
  if(index===scenario.fields.length-1){
    ixTimers.push(window.setTimeout(()=>{
      if(token!==ixEvidenceToken)return;
      view.querySelectorAll('[data-ix-evidence-item]').forEach(row=>row.classList.remove('active'));
      const panel=view.querySelector('[data-ix-verdict]');panel.className=`ix-decision revealed ${scenario.decisionClass}`;
      view.querySelector('[data-ix-decision-body]').innerHTML=`<span class="ix-outcome">${IX_OUTCOME[scenario.decisionClass]||scenario.outcome}</span><strong class="ix-decision-code" data-ix-decision>${ixEscape(scenario.decision)}</strong><span class="ix-rule-name">${ixEscape(scenario.decisionRule)}</span><ul class="ix-checks">${ixPolicyChecks(scenario).map(([text,ok])=>`<li class="${ok?'ok':'no'}">${ixIcon(ok)}<span>${ixEscape(text)}</span></li>`).join('')}</ul><p class="ix-reason">${ixEscape(scenario.decisionReason)}</p><div class="ix-model"><span>Splink probability</span><b>${scenario.probabilityLabel}</b><span>Match weight</span><b>${formatWeight(scenario.matchWeight)}</b></div>`;
      view.querySelector('[data-ix-destination]').innerHTML=ixDestinationMarkup(scenario);
      ixTimers.push(window.setTimeout(()=>{if(token===ixEvidenceToken)ixMarkSettled(IX_RESOLVE)},ixReduced()?60:520));
    },ixReduced()?0:520));
  }
}
function ixRunEvidence(){
  ixEvidenceToken+=1;const token=ixEvidenceToken;ixRenderEvidence();
  if(ixReduced()){ixScenarios[ixActiveScenario].fields.forEach((_,index)=>ixRevealEvidence(index,token));return}
  ixScenarios[ixActiveScenario].fields.forEach((_,index)=>ixTimers.push(window.setTimeout(()=>ixRevealEvidence(index,token),300+index*1050)));
}
// Where the decision lands: Gold in Unity Catalog, which Snowflake reads in place over Iceberg.
function ixDestinationMarkup(scenario){
  const steward=scenario.decisionClass==='review'?`<span><img src="assets/lakebase-icon-full-color.svg" alt="">Queued for a steward in Lakebase</span>`:'';
  return `<span><img src="assets/unity-catalog-icon-full-color.svg" alt="">Published to gold.customer_master</span><span><img src="assets/apache-iceberg-icon-full-color.svg" alt="">Read in Snowflake via Iceberg</span>${steward}`;
}
// Hand-off: one real component from the release. Every record, pair
// decision and customer comes from release-evidence.js `handoff`.
function ixHandoffNodes(){
  return ixHandoff.records.map((r,i)=>{
    const contact=[r.personal_phone,r.personal_email].filter(Boolean);
    const detail=contact.length?contact.join(' · '):[r.home_address_line1,r.home_suburb].filter(Boolean).join(', ')||'No contact details';
    return {key:r.record_key,letter:String.fromCharCode(65+i),source:r.source_system,name:r.full_name,detail,customer:r.customer_id,x:IX_NODE_POS[i][0],y:IX_NODE_POS[i][1],
      contacts:{phone:r.personal_phone,email:r.personal_email}};
  });
}
function ixHandoffMarkup(){
  const nodes=ixHandoffNodes(),by=Object.fromEntries(nodes.map(n=>[n.key,n]));
  const pct=(v,axis)=>`${(v/(axis==='x'?600:400)*100).toFixed(3)}%`;
  const edges=ixHandoff.pairs.map(p=>{
    const a=by[p.left],b=by[p.right],kind=p.decision==='AUTO_MATCH'?'auto':p.decision==='REVIEW'?'review':'reject';
    const shared=a.contacts.phone&&a.contacts.phone===b.contacts.phone?'shared phone':a.contacts.email&&a.contacts.email===b.contacts.email?'shared email':'';
    const label=kind==='auto'?`Auto-match · ${shared}`:kind==='review'?`Review · ${formatProbability(p.match_probability)} · no personal contact`:'';
    return {a,b,kind,label};
  });
  const main=nodes.filter(n=>n.customer===nodes[2].customer),held=nodes.filter(n=>n.customer!==nodes[2].customer);
  // Cards are wider than tall; pad the hulls so every card sits inside its customer.
  const box=list=>{const xs=list.map(n=>n.x),ys=list.map(n=>n.y);return {x:Math.min(...xs)-110,y:Math.min(...ys)-44,w:Math.max(...xs)-Math.min(...xs)+250,h:Math.max(...ys)-Math.min(...ys)+88}};
  const mb=box(main),hb=box(held),h=ixRelease.headline,n=ixFormatCount;
  const edgeSvg=edges.map(e=>`<line class="ix-edge ix-edge-${e.kind}" x1="${e.a.x}" y1="${e.a.y}" x2="${e.b.x}" y2="${e.b.y}" pathLength="1"/>`).join('');
  const edgeLabels=edges.filter(e=>e.label).map(e=>`<span class="ix-edge-label ix-edge-label-${e.kind}" style="left:${pct((e.a.x+e.b.x)/2,'x')};top:${pct((e.a.y+e.b.y)/2-(Math.abs(e.a.y-e.b.y)<20?24:0),'y')}">${ixEscape(e.label)}</span>`).join('');
  const nodeHtml=nodes.map(nd=>`<div class="ix-node" style="left:${pct(nd.x,'x')};top:${pct(nd.y,'y')}"><i>${nd.letter}</i><div><b>${ixEscape(nd.name)}</b><span>${ixEscape(nd.source)} · ${ixEscape(nd.detail)}</span></div></div>`).join('');
  const hull=(b,cls,label)=>`<div class="ix-customer ${cls}" style="left:${pct(b.x,'x')};top:${pct(b.y,'y')};width:${pct(b.w,'x')};height:${pct(b.h,'y')}"><em>${label}</em></div>`;
  return `<div class="ix-handoff-view" data-ix-handoff data-handoff-step="0">
  <section class="ix-graph"><header><b>One component from the release</b><span>${nodes.length} records · ${ixHandoff.pairs.length} scored pairs</span></header>
    <div class="ix-graph-canvas">
      ${hull(mb,'ix-customer-main',`One customer · ${ixEscape(nodes[2].customer.slice(0,12))}… · ${main.length} records`)}
      ${hull(hb,'ix-customer-held','Held apart until a steward decides')}
      <svg viewBox="0 0 600 400" aria-hidden="true">${edgeSvg}</svg>${edgeLabels}${nodeHtml}
    </div>
    <p class="ix-graph-note">A and D never matched directly. C links to both, so the graph joins all three.</p>
    <div class="ix-case"><img src="assets/lakebase-icon-full-color.svg" alt=""><div><b>Steward case · ${ixEscape(nodes[0].name)} · ${nodes.length} records</b><span>Queued in Lakebase for the Steward app</span></div></div>
  </section>
  <aside class="ix-funnel"><header><b>The whole release</b><span>${ixEscape(ixRelease.release.release_id)}</span></header>
    <dl>
      <div data-funnel="1"><dt>${n(h.release_records)}</dt><dd>source records</dd></div>
      <div data-funnel="1"><dt>${n(h.candidate_pairs)}</dt><dd>candidate pairs<small>AI Search ${n(h.ann_pairs)} · blocking ${n(h.deterministic_pairs)} · some by both</small></dd></div>
      <div data-funnel="2" class="ix-f-auto"><dt>${n(h.auto_match_pairs)}</dt><dd>auto-matched pairs</dd></div>
      <div data-funnel="2" class="ix-f-review"><dt>${n(h.review_pairs)}</dt><dd>pairs for review</dd></div>
      <div data-funnel="2" class="ix-f-reject"><dt>${n(h.reject_pairs)}</dt><dd>rejected pairs</dd></div>
      <div data-funnel="3"><dt>${n(h.customers)}</dt><dd><span><img src="assets/unity-catalog-icon-full-color.svg" alt="">customers in gold.customer_master</span></dd></div>
      <div data-funnel="4" class="ix-f-case"><dt>${n(h.steward_cases)}</dt><dd><span><img src="assets/lakebase-icon-full-color.svg" alt="">Steward cases</span><small>Review pairs plus groups the graph held back</small></dd></div>
    </dl>
  </aside>
</div>`}
function ixRunHandoff(){
  const view=ixOverlay.querySelector('[data-ix-handoff]'),set=step=>{view.dataset.handoffStep=String(step)};
  if(ixReduced()){set(4);ixTimers.push(window.setTimeout(()=>ixMarkSettled(IX_HANDOFF),60));return}
  set(0);void view.offsetWidth;set(1);
  [[2,700],[3,2300],[4,3700]].forEach(([step,at])=>ixTimers.push(window.setTimeout(()=>set(step),at)));
  ixTimers.push(window.setTimeout(()=>ixMarkSettled(IX_HANDOFF),ixStageDefinitions[IX_HANDOFF].duration));
}
// Compare: the Reveal-rows table gives way to the ledger (a crossfade; the ledger is
// transposed, so there is no one-to-one geometry to morph).
function ixMorphToEvidence(){
  const token=ixEvidenceToken;ixOverlay.dataset.matchStep='3';
  ixTimers.push(window.setTimeout(()=>{if(token===ixEvidenceToken)ixMarkSettled(IX_COMPARE)},ixReduced()?40:900));
}

// Switching scenario cancels every pending timer, in-flight result chip and
// table animation from the previous scenario before rendering the new one.
function ixSelectScenario(index){
  const stage=ixActiveStage;
  ixClearTimers();ixActiveScenario=index;
  if(stage>=IX_COMPARE)ixOverlay.dataset.matchStep=ixMatchStep(stage);
  ixRenderEvidence();ixMarkPlaying(stage);
  if(stage===IX_RESOLVE)ixTimers.push(window.setTimeout(ixRunEvidence,180));
  else ixTimers.push(window.setTimeout(()=>ixMarkSettled(stage),ixReduced()?40:420));
}
ixOverlay.querySelectorAll('[data-ix-scenario]').forEach(button=>button.addEventListener('click',()=>ixSelectScenario(Number(button.dataset.ixScenario))));
ixRenderEvidence();

function ixLayoutUniverse(){
  const {clientWidth:width,clientHeight:height}=ixOverlay;
  if(!width||!height)return;
  points.forEach(point=>{
    const element=ixOverlay.querySelector(`[data-ix-source-point="${point.id}"]`),geometry=ixHullGeometries[point.cluster],position=projection(point,width,height,STOP_ANGLE,1);
    const clusterLeft=(geometry.cx-geometry.rx)*width,clusterTop=(geometry.cy-geometry.ry)*height;
    element.style.setProperty('--x',`${((position.x-clusterLeft)/(geometry.rx*2*width)*100).toFixed(4)}%`);
    element.style.setProperty('--y',`${((position.y-clusterTop)/(geometry.ry*2*height)*100).toFixed(4)}%`);
  });
}


// The presenter steps stages with the chevrons and keyboard, so the base trial's
// play-all button is removed; its replay control is taken over so it cannot
// desynchronise the stage state.
ixTrial.querySelector('[data-play-all="center"]').remove();
const ixOriginalReplay=ixTrial.querySelector('[data-replay]');
const ixReplay=ixOriginalReplay.cloneNode(true);
ixReplay.removeAttribute('data-replay');ixOriginalReplay.replaceWith(ixReplay);

function ixClearStageTimers(){ixTimers.forEach(timer=>window.clearTimeout(timer));ixTimers=[]}
function ixClearTimers(){ixClearStageTimers();ixEvidenceToken+=1;ixOverlay.classList.remove('ix-table-morphing','ix-widening','ix-morph-landing','ix-tethers-drawn');ixOverlay.querySelectorAll('.ix-table-morph').forEach(element=>element.remove())}
function ixUpdateRail(stage){
  ixActiveStage=stage;ixTrial.dataset.ixStage=String(stage);ixTrial.dataset.stageTitle=ixStageDefinitions[stage]?.title||'Architecture';ixUpdateNav();
}
// Chevrons grey out while a stage animates; when it settles the forward chevron
// darkens and nudges once to invite the next step.
function ixUpdateNav(nudge=false){
  const from=ixActiveStage,name=i=>ixStageDefinitions[i]?.title||'Architecture';
  ixPrev.disabled=from<=IX_INTRO;ixNext.disabled=from>=IX_OUTRO;
  ixPrev.title=from>IX_INTRO?`Back: ${name(from-1)}`:'';ixNext.title=from<IX_OUTRO?`Next: ${name(from+1)}`:'';
  ixShell.classList.toggle('ix-nav-busy',ixTrial.dataset.stageState==='playing');
  ixNext.classList.remove('ix-nudge');
  if(nudge&&!ixNext.disabled&&!ixShell.classList.contains('ix-nav-busy')&&!ixReduced()){void ixNext.offsetWidth;ixNext.classList.add('ix-nudge')}
}
function ixMarkPlaying(stage){ixTrial.dataset.stageState='playing';ixUpdateNav()}
function ixMarkSettled(stage){if(stage!==ixActiveStage)return;ixTrial.dataset.stageState='settled';ixUpdateNav(true)}

function ixSetHeading(eyebrow,title,copy){
  const heading=ixTrial.querySelector('.heading'),h1=heading.querySelector('h1');
  if(h1.textContent===title)return;
  const kicker=heading.querySelector('.eyebrow');if(kicker)kicker.textContent=eyebrow;h1.textContent=title;heading.querySelector('p').textContent=copy;
  heading.classList.remove('ix-swap');void heading.offsetWidth;heading.classList.add('ix-swap');
}
// Before anything plays the page shows its title; from Encode on, each opening stage
// narrates itself in the heading (the old floating caption card is gone).
function ixBlockingHeading(stage){const config=configs[0];if(stage===undefined){ixSetHeading(config.name,config.title,config.copy);return}const [title,copy]=narration[IX_TRIAL_STAGE[stage]];ixSetHeading(config.name,title,copy)}
function ixMatchHeading(stage){
  const copy={
    4:['Candidate selection','Take one candidate pair. Being close is not a match.',`${ixHero.routeDetail} Retrieval only proposes the comparison.`],
    5:['Post-blocking matching','Behind every point is a source record.','These are the two rows the points stand for, exactly as each source system holds them.'],
    6:['Field evidence','Splink compares the pair field by field.',`Each comparison adds or subtracts match weight, learned from how often values agree across all guests. All six scenarios are real pairs from release ${ixRelease.release.release_id}.`],
    7:['Policy decision','The match policy makes the call, not the score alone.','Splink’s probability is evidence. decide_v4, a versioned match policy, turns it into an automatic link, a rejection, or a case for a steward.'],
    8:['Customer graph','Matches join into customers. What is left goes to a steward.','Auto-matched pairs join records into customers, even through a third record. Review pairs, and joins the graph refuses, such as two birth dates in one guest, become Steward cases in Lakebase.']
  }[stage];
  ixSetHeading(copy[0],copy[1],copy[2]);
}
function ixResetMatching(){ixOverlay.querySelector('[data-ix-handoff]').dataset.handoffStep='0';ixTrial.classList.remove('integrated-matching');ixTrial.removeAttribute('data-match-stage');ixOverlay.dataset.matchStep='0'}
function ixShowBlocking(stage){
  ixClearTimers();ixResetMatching();ixBlockingHeading(stage);ixUpdateRail(stage);ixMarkPlaying(stage);play('center',IX_TRIAL_STAGE[stage],false);
  const duration=ixStageDefinitions[stage].duration;
  if(ixReduced()){state.center.start=performance.now()-duration;ixTimers.push(window.setTimeout(()=>ixMarkSettled(stage),80))}
  else ixTimers.push(window.setTimeout(()=>ixMarkSettled(stage),duration+(stage===0?0:120)));
}
function ixShowMatching(stage){
  ixClearTimers();stop('center');state.center.stage=4;state.center.progress=1;state.center.playing=false;state.center.dirty=true;ixTrial.dataset.stage='4';ixTrial.dataset.encodePhase='vector';
  ixLayoutUniverse();ixTrial.classList.add('integrated-matching');ixTrial.dataset.matchStage=String(stage);ixOverlay.dataset.matchStep='0';ixMatchHeading(stage);ixUpdateRail(stage);ixMarkPlaying(stage);
  if(stage===IX_COMPARE){ixActiveScenario=0;ixRenderEvidence()}
  ixTimers.push(window.setTimeout(()=>{window.requestAnimationFrame(()=>{
    if(stage===IX_COMPARE){ixMorphToEvidence();return}
    ixOverlay.dataset.matchStep=ixMatchStep(stage);
    // Arrowheads appear once the tethers have traced in.
    if(stage===IX_REVEAL)ixTimers.push(window.setTimeout(()=>ixOverlay.classList.add('ix-tethers-drawn'),ixReduced()?0:950));
    if(stage===IX_RESOLVE){ixRunEvidence();return}
    if(stage===IX_HANDOFF){ixRunHandoff();return}
    ixTimers.push(window.setTimeout(()=>ixMarkSettled(stage),ixReduced()?60:1000));
  })},stage===IX_SELECT?60:0));
}
function ixShowStage(stage){if(stage===IX_INTRO||stage===IX_OUTRO){ixShowArch(stage);return}ixTrial.classList.remove('ix-arch-on','ix-arch-outro');if(stage<IX_SELECT)ixShowBlocking(stage);else ixShowMatching(stage)}


// Architecture: the opening view (before Encode) and the closing view (after Hand off).
// The closing view repeats the diagram with the release's numbers on its arrows.
const IX_INTRO=-1,IX_OUTRO=ixStageDefinitions.length;
const IX_ARCH_COPY={
  [IX_INTRO]:['Guest identity resolution on Databricks.','Guest records come from Snowflake, are resolved on Databricks, and go back to Snowflake as one golden record per guest. What the pipeline cannot decide safely goes to a steward.'],
  [IX_OUTRO]:[`From ${ixFormatCount(ixRelease.headline.release_records)} records to ${ixFormatCount(ixRelease.headline.customers)} guests.`,'The pipeline resolves what it can on its own. Stewards and the agent resolve the rest in the app, and Snowflake reads the result in place.']
};
// Snowflake's mark (Simple Icons, from snowflake.com/brand-guidelines), brand blue #29B5E8.
const ixSnowflake='<img class="ix-snow" src="assets/snowflake-icon.svg" alt="">';
function ixArchMarkup(){
  const h=ixRelease.headline,n=ixFormatCount;
  const node=(key,icon,title,sub,stat)=>`<div class="ix-arch-node${icon?'':' ix-arch-plain'}" data-arch="${key}">${icon?`<img src="${icon}" alt="">`:''}<b>${title}</b><span>${sub}</span>${stat?`<em>${stat}</em>`:''}</div>`;
  // viewBox 1000x380: main row at y=250, app row at y=146.
  const edges=[['in','M112 250 H181',[181,250,'r']],['q','M319 250 H351',[351,250,'r']],['s','M489 250 H521',[521,250,'r']],['gold','M667 250 H731',[731,250,'r']],
    ['app','M594 221 V146 H631',[631,146,'r']],['back','M811 146 H822 V219',[822,219,'d']],['out','M859 250 H906',[906,250,'r']]];
  const head=([x,y,dir])=>dir==='r'?`M${x-8} ${y-4.5} L${x} ${y} L${x-8} ${y+4.5}Z`:`M${x-4.5} ${y-8} L${x} ${y} L${x+4.5} ${y-8}Z`;
  return `<div class="ix-arch" data-arch-step="0" aria-label="Architecture"><div class="ix-arch-stage">
  <svg class="ix-arch-lines" viewBox="0 0 1000 380" aria-hidden="true">${edges.map(([k,d,tip])=>`<path class="ix-arch-edge" data-edge="${k}" d="${d}" pathLength="1"/><path class="ix-arch-head" data-edge="${k}" d="${head(tip)}"/>`).join('')}</svg>
  <div class="ix-arch-ext ix-arch-src">${ixSnowflake}<b>Snowflake</b><span>Source guest records</span><em>${n(h.release_records)} records</em></div>
  <section class="ix-arch-box"><header><img src="assets/databricks-icon-full-color.svg" alt=""><b>Databricks</b><span>Governed by Unity Catalog</span></header>
    ${node('query','','AI Query','Embed names and addresses in SQL')}
    ${node('search','assets/ai-search-icon-full-color.svg','AI Search','Nearest neighbours',`${n(h.candidate_pairs)} pairs`)}
    ${node('match','','Splink + match policy','Score each pair, then decide')}
    ${node('app','assets/apps-icon-full-color.svg','Steward app + agent','Lakebase · Unity AI Gateway',`${n(h.steward_cases)} cases`)}
    ${node('gold','assets/unity-catalog-icon-full-color.svg','Gold record','gold.customer_master',`${n(h.customers)} guests`)}
    <div class="ix-arch-jobs"><i></i><span><img src="assets/lakeflow-jobs-icon-full-color.svg" alt="">Lakeflow Jobs · one pipeline job</span></div>
  </section>
  <div class="ix-arch-ext ix-arch-dst">${ixSnowflake}<b>Snowflake</b><span>Reads Gold in place via Iceberg</span></div>
</div></div>`}
ixDemo.insertAdjacentHTML('beforeend',ixArchMarkup());
const ixArch=ixDemo.querySelector('.ix-arch');
function ixShowArch(stage){
  ixClearTimers();ixResetMatching();stop('center');
  ixTrial.classList.add('ix-arch-on');ixTrial.classList.toggle('ix-arch-outro',stage===IX_OUTRO);
  const [title,copy]=IX_ARCH_COPY[stage];ixSetHeading('',title,copy);ixUpdateRail(stage);ixMarkPlaying(stage);
  if(ixReduced()){ixArch.dataset.archStep='9';ixTimers.push(window.setTimeout(()=>ixMarkSettled(stage),60));return}
  ixArch.dataset.archStep='0';void ixArch.offsetWidth;
  // Left to right along the flow: source, then each step, then back out to Snowflake.
  [[1,80],[2,420],[3,760],[4,1100],[5,1440],[6,1780],[7,2120]].forEach(([step,at])=>ixTimers.push(window.setTimeout(()=>{ixArch.dataset.archStep=String(step)},at)));
  if(stage===IX_OUTRO)ixTimers.push(window.setTimeout(()=>{ixArch.dataset.archStep='9'},2600));
  ixTimers.push(window.setTimeout(()=>ixMarkSettled(stage),stage===IX_OUTRO?3100:2500));
}

function ixGoTo(stage){ixShowStage(stage)}
ixPrev.addEventListener('click',()=>ixGoTo(Math.max(IX_INTRO,ixActiveStage-1)));
ixNext.addEventListener('click',()=>ixGoTo(Math.min(IX_OUTRO,ixActiveStage+1)));
ixReplay.addEventListener('click',()=>ixGoTo(3));

// Presenter keyboard: arrows / PageUp / PageDown / Space step, Home / End jump,
// digits 1-9 open a stage. Captured before the base trial's arrow handler.
window.addEventListener('keydown',event=>{
  if(event.metaKey||event.ctrlKey||event.altKey)return;
  const target=event.target;
  if(target&&(target.isContentEditable||/^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName)))return;
  const onButton=target instanceof HTMLButtonElement;
  const from=ixActiveStage;
  let next=null;
  if(event.key==='ArrowRight'||event.key==='PageDown'||(event.key===' '&&!onButton))next=Math.min(IX_OUTRO,from+1);
  else if(event.key==='ArrowLeft'||event.key==='PageUp')next=Math.max(IX_INTRO,from-1);
  else if(event.key==='Home')next=IX_INTRO;
  else if(event.key==='End')next=IX_OUTRO;
  else if(/^[1-9]$/.test(event.key))next=Number(event.key)-1;
  if(event.key==='ArrowLeft'||event.key==='ArrowRight')event.stopImmediatePropagation();
  if(next===null)return;
  event.preventDefault();
  if(next===ixActiveStage&&/Arrow|Page| /.test(event.key))return;
  ixGoTo(next);
},true);

function ixCentre(element,root,scale){const box=element.getBoundingClientRect(),base=root.getBoundingClientRect();return{x:(box.left-base.left+box.width/2)/scale,y:(box.top-base.top+box.height/2)/scale}}
// Tethers follow their moving endpoints only while Reveal rows is animating
// (plus a few frames after it settles, and after a resize); a settled stage
// does no per-frame layout work.
let ixTetherFrames=0;
function ixTetherFrame(){
  const active=ixTrial.classList.contains('integrated-matching')&&ixOverlay.dataset.matchStep==='2'&&!ixOverlay.classList.contains('ix-table-morphing');
  if(active&&ixTrial.dataset.stageState!=='settled')ixTetherFrames=4;
  if(active&&ixTetherFrames>0){ixTetherFrames-=1;
    const scale=ixVisualScale(),svg=ixOverlay.querySelector('.ix-tethers');svg.setAttribute('viewBox',`0 0 ${ixDemo.clientWidth} ${ixDemo.clientHeight}`);
    ['left','right'].forEach(side=>{const start=ixCentre(ixOverlay.querySelector(`[data-ix-point="${side}"] i`),ixDemo,scale),end=ixCentre(ixOverlay.querySelector(`[data-ix-row="${side}"] .ix-row-anchor`),ixDemo,scale),dx=end.x-start.x;// Curves arrive horizontally, so each arrowhead is a right-pointing triangle at the row edge.
      ixOverlay.querySelector(`[data-ix-arrow="${side}"]`).setAttribute('d',`M ${(end.x-9).toFixed(1)} ${(end.y-5).toFixed(1)} L ${end.x.toFixed(1)} ${end.y.toFixed(1)} L ${(end.x-9).toFixed(1)} ${(end.y+5).toFixed(1)} Z`);
      ixOverlay.querySelector(`[data-ix-tether="${side}"]`).setAttribute('d',`M ${start.x.toFixed(1)} ${start.y.toFixed(1)} C ${(start.x+dx*.55).toFixed(1)} ${start.y.toFixed(1)}, ${(end.x-dx*.55).toFixed(1)} ${end.y.toFixed(1)}, ${end.x.toFixed(1)} ${end.y.toFixed(1)}`)});
  }
  window.requestAnimationFrame(ixTetherFrame);
}

window.addEventListener('resize',()=>{ixTetherFrames=4;ixFitFrame();ixLayoutUniverse()});
ixLayoutUniverse();ixShowArch(IX_INTRO);window.requestAnimationFrame(ixTetherFrame);
