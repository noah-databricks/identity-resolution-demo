import {patchChildren} from './dom-patch.js';
import {mountGuestEditor, FIELDS, letter} from './guest-editor.js';
import {modelEvidenceMarkup, pairBadge, queueScore, ruleChecksMarkup, weightScale, whyReviewMarkup} from './model-evidence.js';
const $ = selector => document.querySelector(selector);
function toolIcon(tool) {
  const paths={get_case_context:'M3 3h18v18H3z M3 9h18 M9 9v12',get_pipeline_observations:'M10 3a7 7 0 1 0 0 14 7 7 0 0 0 0-14 M15 15l6 6',done:'M20 6 9 17l-5-5',preview_resolution:'M8 5h8 M6 7v10 M18 7v10 M8 19h8 M4 3h4v4H4z M16 3h4v4h-4z M4 17h4v4H4z M16 17h4v4h-4z'};
  return `<svg class="tool-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="${paths[tool]||'M4 6h16 M4 12h16 M4 18h16'}"/></svg>`;
}
const calm = () => matchMedia('(prefers-reduced-motion: reduce)').matches;
function enter(node) {
  if(!calm()) node.animate([{opacity:0,transform:'translateY(6px)'},{opacity:1,transform:'none'}],{duration:180,easing:'cubic-bezier(.2,.7,.3,1)'});
}
const LOCALE = 'en-AU';
const PAGE = 30;
// Probabilities can be small; keep enough precision that they never read as 0.
const score = value => Number(value).toLocaleString(LOCALE,{maximumFractionDigits:Math.abs(value)<.1&&value!=0?3:2});
const FAILURE_TEXT = {RATE_LIMITED:'The model endpoint is at its per-minute token limit. Wait a minute, then send the request again.',TIMEOUT:'The review took longer than 90 seconds and was stopped. Send the request again.',INTERRUPTED:'The review was interrupted by an app restart. Send the request again.',INVALID_RESULT:'The agent returned a recommendation that failed validation. Send the request again or review manually.'};
const TOOL_TITLES = {get_case_context:'Read source records',get_pipeline_observations:'Assess pipeline observations',preview_resolution:'Validate proposed groups'};
function reveal(node) {
  if(!matchMedia('(prefers-reduced-motion: reduce)').matches) node.animate([{opacity:.65},{opacity:1}],{duration:160,easing:'ease-out'});
}
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const label = value => String(value ?? '').replaceAll('_', ' ').toLowerCase();
const actions = {MERGE:'Merge records', SEPARATE:'Keep separate', PARTITION:'Split into groups', DEFER:'Defer review'};
let selected = null, current = null, offset = 0, queueEpoch = 0, caseEpoch = 0;
let proposal = null, manualAction = null, busy = false, polling = null, runs = [], pendingSubmit = null;
let guestEditor = null, leanFilter = 'all';
const LEAN_TEXT = {same:'Likely same guest', different:'Possibly different people'};
const runViews = new Map();
const liveStreams = new Map();
let noticeTimer;

function notice(message) {
  clearTimeout(noticeTimer); const node=$('#notice'), wasHidden=node.hidden; node.textContent = message; node.hidden = false; if(wasHidden) enter(node);
  noticeTimer = setTimeout(() => { $('#notice').hidden = true; }, 8000);
}
async function api(path, body) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);
  try {
    const response = await fetch(path, {credentials:'same-origin', signal:controller.signal,
      headers: body ? {'Content-Type':'application/json','X-Steward-Request':'1'} : {},
      ...(body ? {method:'POST', body:JSON.stringify(body)} : {})});
    if (!response.headers.get('content-type')?.includes('application/json')) throw new Error('Sign-in expired. Reload this page to sign in.');
    const result = await response.json();
    if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : `Request failed (${response.status}). Refresh and retry.`);
    return result;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error('Request timed out. Refresh to check whether it was saved before retrying.');
    throw error;
  } finally { clearTimeout(timeout); }
}
function recordName(record) { return record.name || `${record.source_system} · ${String(record.source_record_id).slice(0,8)}`; }
function recordLabel(key) {
  const index = current?.records.findIndex(r=>r.record_key===key) ?? -1;
  const record = current?.records[index];
  return record ? `${letter(index)} · ${record.source_system}` : `Record ${String(key).slice(0,8)}`;
}
function groupsMarkup(groups) {
  // Past six records a list of labels stops being readable; summarise by source.
  const bySource = group => Object.entries(group.reduce((n,key)=>{const s=current?.records.find(r=>r.record_key===key)?.source_system||'unknown';n[s]=(n[s]||0)+1;return n;},{})).sort((a,b)=>b[1]-a[1]).map(([s,c])=>`${c} ${s}`).join(', ');
  return groups.map((group,i) => `<div class="group"><strong>Guest ${i+1}</strong> · ${group.length>6?`${group.length} records: ${esc(bySource(group))}`:group.map(key => `<span title="${esc(key)}">${esc(recordLabel(key))}</span>`).join(' + ')}</div>`).join('');
}
async function loadSummary() {
  try {
    const data = await api('/api/summary');
    const c = data.case_counts;
    $('#metrics').innerHTML = `<span><b>${Number(c.REVIEW||0).toLocaleString()}</b> in review</span><span><b>${Number(c.DEFERRED||0).toLocaleString()}</b> deferred</span><span><b>${Number(c.RESOLVED||0).toLocaleString()}</b> steward-resolved</span>`;
    const release=data.release, chip=$('#release');
    $('#metrics').title = release ? `Lakebase · imported ${new Date(release.imported_at).toLocaleString(LOCALE)}` : 'Lakebase · no resolver release imported';
    chip.hidden=false;
    chip.innerHTML = release ? `<span>Release</span> <b>${esc(release.source_run_id)}</b>` : '<span>No release imported</span>';
    chip.title = release ? [`Active resolver release ${release.source_run_id}`, release.resolver_version&&`Resolver ${release.resolver_version}`,
      `${Number(release.summary?.cases||0).toLocaleString(LOCALE)} cases from ${Number(release.summary?.source_records||0).toLocaleString(LOCALE)} source records`,
      `Imported ${new Date(release.imported_at).toLocaleString(LOCALE)}`,
      release.summary?.fingerprint&&`Fingerprint ${release.summary.fingerprint.slice(0,12)}`,
      release.metadata?.deferred_oversized_cases?`${plural(release.metadata.deferred_oversized_cases,'oversized case')} (${plural(release.metadata.deferred_oversized_records||0,'record')}) held outside this queue; Gold marks them for review`:null,
      data.archived_releases?`${data.archived_releases} earlier release${data.archived_releases===1?'':'s'} archived`:null].filter(Boolean).join('\n') : 'Import a resolver release to start reviewing.';
  } catch (error) { $('#metrics').textContent = 'Queue summary unavailable'; $('#metrics').title = error.message; }
}
async function loadQueue() {
  const epoch = ++queueEpoch;
  $('#queue-list').setAttribute('aria-busy','true');
  if(!$('#queue-list').children.length)$('#queue-list').innerHTML=`<span class="sr-only" role="status">Loading review queue</span><div class="case-skeleton queue-skeleton" aria-hidden="true">${Array.from({length:7},()=>'<div><span class="skeleton-line" style="width:62%"></span><span class="skeleton-line" style="width:84%"></span><span class="skeleton-line" style="width:40%"></span></div>').join('')}</div>`;
  $('#previous').disabled = $('#next').disabled = true;
  try {
    const data = await api(`/api/cases?status=${$('#status').value}&sort=${$('#sort').value}&search=${encodeURIComponent($('#search').value.trim())}&lean=${leanFilter}&offset=${offset}&limit=${PAGE}`);
    if (epoch !== queueEpoch) return;
    $('#queue-list').innerHTML = data.items.map(item => `<button class="case" data-case="${esc(item.case_id)}" aria-pressed="${item.case_id===selected}"><span class="case-top"><strong>${esc(recordName(item.records[0]))}</strong>${(q=>`<span class="score" title="${esc(q.title)}">${esc(q.text.replace(/^Match /,''))}</span>`)(queueScore(item,score))}</span><small>${plural(item.records.length,'record')} · ${esc([...new Set(item.records.map(r=>r.source_system))].join(', '))}</small>${LEAN_TEXT[item.lean?.kind]?`<span class="lean lean-${item.lean.kind}" title="${esc((item.lean.signals||[]).join(' · ')||'No conflicting evidence; every review pair scored 95% or higher')}">${LEAN_TEXT[item.lean.kind]}</span>`:''}${(item.review_tags||[]).length?`<span class="tags">${item.review_tags.map(esc).join(' · ')}</span>`:''}</button>`).join('') || emptyQueue(data.total);
    const n=value=>Number(value).toLocaleString(LOCALE);
    Object.entries(data.lean_counts||{}).forEach(([kind,count])=>{const node=document.querySelector(`[data-lean-count="${kind}"]`);if(node)node.textContent=n(count);});
    $('#page').textContent = data.items.length ? `${n(offset+1)}–${n(offset+data.items.length)} of ${n(data.total)}` : `0 of ${n(data.total)}`;
    if(!data.items.length&&offset>0&&data.total>0){offset=Math.max(0,Math.floor((data.total-1)/PAGE)*PAGE);loadQueue();return;}
    $('#previous').disabled = offset===0; $('#next').disabled = offset+data.items.length >= data.total;
    reveal($('#queue-list'));
    document.querySelectorAll('[data-case]').forEach(button => button.onclick = () => loadCase(button.dataset.case));
  } catch (error) { if (epoch===queueEpoch) $('#queue-list').innerHTML = `<div class="error">${esc(error.message)}</div>`; }
  finally {if(epoch===queueEpoch)$('#queue-list').removeAttribute('aria-busy');}
}
function plural(n, noun) { return `${Number(n).toLocaleString(LOCALE)} ${noun}${n===1?'':'s'}`; }
function emptyQueue(total) {
  const search=$('#search').value.trim(), status=$('#status').value;
  if(search) return `<div class="empty">No ${status==='REVIEW'?'cases in review':status.toLowerCase()+' cases'} match “${esc(search)}”.<br>Search matches guest names; clear it to see every case.</div>`;
  if(status==='DEFERRED') return '<div class="empty">No deferred cases.<br>Cases a steward defers appear here.</div>';
  if(status==='RESOLVED') return '<div class="empty">No steward-resolved cases yet.<br>Confirmed decisions appear here.</div>';
  return total===0 ? '<div class="empty">No cases in review.<br>Refresh after a resolver release is imported.</div>' : '<div class="empty">No cases on this page.</div>';
}
function controls() {
  const enabled = !!current && !busy;
  $('#message').disabled = !enabled || $('#mode').value==='manual';
  $('#send').disabled = !enabled || $('#mode').value==='manual';
  $('#manual').classList.toggle('expanded',!!manualAction);
  $('#preview-manual').disabled = !enabled || !manualAction || !$('#rationale').value.trim();
  document.querySelectorAll('[data-action]').forEach(b => b.disabled = !enabled);
  $('#mode-note').textContent = $('#mode').value==='auto' ? 'Auto-approve: sending a request permits the agent to save its validated decision without confirmation.' : $('#mode').value==='manual' ? 'Choose a manual decision in Source comparison.' : 'The agent proposes changes. You confirm the exact proposal before it is saved.';
}
function resetCase(soft=false) {
  guestEditor=null;
  liveStreams.forEach(stream=>stream.close());liveStreams.clear();
  clearTimeout(polling); runViews.clear(); runs=[]; current=null; proposal=null; manualAction=null;
  $('#proposal').hidden=true; $('#partition').hidden=true; $('#recommended').textContent='';
  if(!soft){$('#manual').hidden=true;$('#rationale').value='';$('#message').value='';}
  $('#agent-state').textContent=''; controls();
  document.querySelectorAll('[data-action]').forEach(b=>{b.classList.remove('recommended');b.setAttribute('aria-pressed','false');});
}
async function loadCase(id, {soft=false}={}) {
  const epoch = ++caseEpoch;
  const keep = soft && id===selected && guestEditor ? {draft:{...guestEditor.draft.snapshot(),history:guestEditor.draft.history}, action:manualAction} : null;
  selected=id; resetCase(!!keep);
  if(keep) { for(const selector of ['#evidence','#conversation'])$(selector).setAttribute('aria-busy','true'); }
  else { showCaseSkeleton($('#evidence'), 'evidence'); showCaseSkeleton($('#conversation'), 'history'); }
  document.querySelectorAll('[data-case]').forEach(b=>b.setAttribute('aria-pressed',b.dataset.case===id));
  try {
    const data=await api(`/api/cases/${encodeURIComponent(id)}`);
    if(epoch!==caseEpoch)return;
    current=data; renderEvidence();
    if(keep){guestEditor.restore(keep.draft);manualAction=keep.action;document.querySelectorAll('[data-action]').forEach(b=>b.setAttribute('aria-pressed',b.dataset.action===manualAction));}
    controls();
    $('#evidence').removeAttribute('aria-busy');
    const history=await api(`/api/cases/${encodeURIComponent(id)}/agent-runs`);
    if(epoch!==caseEpoch)return;
    runs=history.items.reverse();
    await pollRuns(epoch);
  } catch(error) { if(epoch===caseEpoch) {$('#conversation').innerHTML=`<div class="error">${esc(error.message)}</div>`; if(!current)$('#evidence').innerHTML='<div class="empty">Source evidence unavailable. Refresh to retry.</div>'; } }
  finally {if(epoch===caseEpoch)for(const selector of ['#evidence','#conversation'])$(selector).removeAttribute('aria-busy');}
}
function showCaseSkeleton(node, kind) {
  const line = width => `<span class="skeleton-line" style="width:${width}%"></span>`;
  const content = kind==='evidence'
    ? `${line(42)}${line(28)}<div class="skeleton-table">${Array.from({length:12},()=>`<div>${line(35)}${line(78)}</div>`).join('')}</div>`
    : `<div class="skeleton-message">${line(74)}${line(48)}</div><div class="skeleton-response">${line(32)}${line(88)}${line(94)}${line(66)}</div>`;
  node.setAttribute('aria-busy','true');
  node.scrollTop=0;
  node.innerHTML=`<span class="sr-only" role="status">Loading ${kind==='evidence'?'source evidence':'review history'}</span><div class="case-skeleton" aria-hidden="true">${content}</div>`;
}
function renderEvidence() {
  const fields=[['Name','name'],['Given name','given_name'],['Family name','family_name'],['Personal email','email'],['Personal phone','phone'],['Date of birth','date_of_birth'],['Home address','address'],['Work email','work_email'],['Work phone','work_phone'],['Work address','work_address'],['Company','company_name'],['Source context','contact_context']];
  $('#evidence').innerHTML=`<div class="case-heading"><h1>${esc(recordName(current.records[0]))}</h1><p>${plural(current.records.length,'record')} · case version ${current.version}</p><p>Resolver run ${esc(current.source_run_id)}</p></div>${whyReviewMarkup(current.review_reasons,esc,recordLabel)}<div class="comparison-wrap"><table class="comparison"><thead><tr>${current.records.map(r=>`<th><span class="table-icon" aria-hidden="true"></span>${esc(recordLabel(r.record_key))}<small title="${esc(r.source_record_id)}">${esc(String(r.source_record_id).slice(0,8))}</small><small>${r.source_updated_at ? 'Updated '+esc(new Date(r.source_updated_at).toLocaleString(LOCALE)) : 'Source update time unavailable'}</small></th>`).join('')}</tr></thead><tbody>${fields.filter(([,key])=>current.records.some(r=>r[key]!=null)).map(([title,key])=>`<tr>${current.records.map(r=>`<td><small>${esc(title)}</small>${r[key] == null ? '<span class="muted">Not provided</span>' : esc(r[key])}</td>`).join('')}</tr>`).join('')}</tbody></table></div><h3 class="section-title">Current guest groups</h3>${groupsMarkup(current.current_groups)}<h3 class="section-title">Pair evidence</h3>${pairEvidenceMarkup(current.pair_evidence)}`;
  const comparison=$('#evidence .comparison-wrap'), host=document.createElement('div');
  host.className='guest-editor';comparison.replaceWith(host);
  let old=host.nextElementSibling;
  if(old?.textContent==='Current guest groups'){let next=old.nextElementSibling;old.remove();while(next?.classList.contains('group')){old=next;next=next.nextElementSibling;old.remove();}}
  guestEditor=mountGuestEditor(host,current,draft=>{
    manualAction=draft.action();proposal=null;renderProposal();$('#mode').value='manual';
    document.querySelectorAll('[data-action]').forEach(b=>b.setAttribute('aria-pressed',b.dataset.action===manualAction));
    controls();
  },()=>busy,patchChildren);
  document.querySelectorAll('#evidence [data-open-case]').forEach(b=>b.onclick=()=>loadCase(b.dataset.openCase));
  if($('#manual').hidden){$('#manual').hidden=false;enter($('#manual'));}
}
// Splink releases generate "DECISION: RULE (Splink p=...)", which the routing line already says.
const restatesRule = pair => !!pair.model_evidence?.routing_rule && String(pair.decision_explanation||'').startsWith(`${pair.decision}: ${pair.model_evidence.routing_rule}`);
function pairEvidenceMarkup(pairs) {
  if(!pairs.length)return '<p class="muted">No pair-level evidence supplied.</p>';
  pairs=[...pairs].sort((a,b)=>(b.decision==='REVIEW')-(a.decision==='REVIEW'));
  const scale=weightScale(pairs), firstReview=pairs.findIndex(p=>p.model_evidence&&p.decision==='REVIEW');
  return pairs.map((pair,i)=>{
    // Probabilistic releases leave the rule-based features they do not compute empty.
    const shown=pair.model_evidence?pair.features.filter(([,v])=>v!==null):pair.features;
    const hidden=pair.features.length-shown.length;
    return `<details class="evidence-pair"${i===firstReview?' open':''}><summary>${esc(recordLabel(pair.left_record_key))} ↔ ${esc(recordLabel(pair.right_record_key))} · ${esc(label(pair.decision))}${pairBadge(pair)}</summary>${modelEvidenceMarkup(pair,esc,scale)}${restatesRule(pair)?'':`<p>${esc(pair.decision_explanation || 'No explanation available')}</p>`}<p class="caption">${esc(pair.resolver_version)}${pair.model_evidence?'':` · evidence score ${score(pair.evidence_score)}`}</p>${pair.model_evidence?ruleChecksMarkup(pair,esc,recordLabel):`${shown.length?`<table class="features"><tbody>${shown.map(([key,value])=>`<tr><td>${esc(label(key))}</td><td>${value===null?'Not evaluated':esc(value)}</td></tr>`).join('')}</tbody></table>`:''}${hidden?`<p class="caption">${hidden} rule-based features not supplied by this resolver.</p>`:''}`}</details>`;
  }).join('');
}
function renderConversation() {
  const node=$('#conversation'), nearBottom=node.scrollHeight-node.scrollTop-node.clientHeight<60;
  const next=document.createElement('template');
  next.innerHTML = runs.map(run=>{
    const view=runViews.get(run.run_id);
    return `<article class="run" data-render-key="${esc(run.run_id)}"><div class="user-message">${esc(run.message)}</div><div class="run-heading"><span>Review agent</span><span>${esc(label(view?.status||run.status))}</span></div>${progressMarkup(view,run)}${view?.proposal?`<p class="answer">${esc(view.proposal.rationale)}</p>`+'<p class="caption">AI-generated recommendation · verify against source evidence.</p>':''}${view?.status==='FAILED'?`<p class="error">${esc(FAILURE_TEXT[view.error_code]||`Review failed (${view.error_code}).`)} No decision was recorded.</p>`:''}${view?.trace_id?`<details class="tool" data-render-key="trace"><summary>Trace</summary><pre>${esc(view.trace_id)}</pre></details>`:''}</article>`;
  }).join('') || '<div class="empty">No agent review yet. Send a request below or make a manual decision alongside the source evidence.<p class="caption">Each request reviews the current case snapshot.</p></div>';
  patchChildren(node,next.content);
  if(nearBottom)node.scrollTop=node.scrollHeight;
}
function toolLog(view) {
  const results=(view?.events||[]).filter(e=>e.type==='tool_result');
  if(!results.length)return '';
  const body=results.map(e=>`<div class="tool-call">${toolIcon(e.payload.tool)}<div><strong>${esc(TOOL_TITLES[e.payload.tool]||label(e.payload.tool||'tool'))}</strong>${`<pre>${esc(JSON.stringify({input:e.payload.input,output:e.payload.output},null,2))}</pre>`}</div></div>`).join('');
  return `<details class="tool" data-render-key="tool-log"><summary>${toolIcon('get_case_context')}Tool calls · ${results.length}</summary>${body}</details>`;
}
function progressMarkup(view, run) {
  const status=view?.status||run.status, events=view?.events||[];
  const terminal=['COMPLETED','FAILED'].includes(status);
  const tasks=[['get_case_context','Read source records'],['get_pipeline_observations','Assess pipeline observations'],['preview_resolution','Validate proposed groups']];
  const finished=status==='COMPLETED'&&tasks.every(([tool])=>events.some(e=>e.type==='tool_result'&&e.payload.tool===tool));
  const rows=finished?'':tasks.map(([tool,title])=>{
    const latest=events.filter(e=>e.type.startsWith('tool_')&&e.payload.tool===tool).at(-1);
    const started=!!latest;
    const done=latest?.type==='tool_result';
    const state=done?'done':started?(terminal?'stopped':'running'):'pending';
    return `<div class="task ${state}" data-render-key="${tool}">${toolIcon(tool)}<span>${title}</span><small>${done?'Done':started?(terminal?'Stopped':'Running'):'Pending'}</small></div>`;
  }).join('');
  const streamed=events.filter(e=>e.type==='text_snapshot').at(-1)?.payload;
  const prose=streamed?.text;
  const phase=status==='QUEUED'?'Queued':status==='FAILED'?'Review failed':status==='COMPLETED'?(finished?`Review complete · ${tasks.length} checks done`:'Review complete'):prose?'Writing recommendation':'Reviewing evidence';
  return `<div class="task-progress" role="status" aria-live="polite"><div class="progress-heading${finished?' finished':''}">${finished?toolIcon('done'):''}${!terminal?'<span class="task-spinner" aria-hidden="true"></span>':''}${phase}</div>${rows}</div>${toolLog(view)}${prose&&!view?.proposal?`<p class="answer streaming">${esc(prose)}</p><p class="caption">${terminal?'Incomplete response · no decision saved':'Draft response · awaiting validation'}</p>`:''}`;
}
function streamRun(run, epoch) {
  if(liveStreams.has(run.run_id))return;
  const cursor=runViews.get(run.run_id)?.next_cursor||0;
  const stream=new EventSource(`/api/agent-runs/${encodeURIComponent(run.run_id)}/stream?after=${cursor}`);
  liveStreams.set(run.run_id,stream);
  stream.addEventListener('update',event=>{
    if(epoch!==caseEpoch){stream.close();return;}
    const view=JSON.parse(event.data),old=runViews.get(run.run_id);
    const events=new Map([...(old?.events||[]),...view.events].map(e=>[e.sequence,e]));
    const terminal=['COMPLETED','FAILED'].includes(view.status)&&view.events.length<100;
    runViews.set(run.run_id,{...view,status:terminal?'RUNNING':view.status,events:[...events.values()]});
    renderConversation();
    if(terminal){stream.close();liveStreams.delete(run.run_id);pollRuns(epoch);}
  });
  stream.onerror=()=>{
    stream.close();liveStreams.delete(run.run_id);
    if(epoch===caseEpoch){$('#agent-state').textContent='Reconnecting…';polling=setTimeout(()=>pollRuns(epoch),1500);}
  };
}
async function pollRuns(epoch) {
  clearTimeout(polling); let active=false;
  try {
    for(const run of runs) {
      const old=runViews.get(run.run_id);
      if(old && ['COMPLETED','FAILED'].includes(old.status) && !old.more)continue;
      const view=await api(`/api/agent-runs/${encodeURIComponent(run.run_id)}?after=${old?.next_cursor||0}`);
      if(epoch!==caseEpoch)return;
      runViews.set(run.run_id,{...view,more:view.events.length===100,events:[...(old?.events||[]),...view.events]});
      if(['QUEUED','RUNNING'].includes(view.status)||view.events.length===100)active=true;
      if(view.status==='COMPLETED' && run===runs.at(-1) && view.proposal && !manualAction) {
        if(view.mode==='auto') {
          $('#agent-state').textContent='Decision saved';
          const updated=await api(`/api/cases/${encodeURIComponent(selected)}`);
          if(epoch!==caseEpoch)return;current=updated;renderEvidence();loadSummary();loadQueue();
        } else if(view.proposal.case_version===current.version) {proposal=view.proposal;renderProposal();}
      }
    }
    if(epoch!==caseEpoch)return;
    $('#agent-state').textContent=active?'Review in progress':'';renderConversation();
    if(active)runs.filter(r=>['QUEUED','RUNNING'].includes(runViews.get(r.run_id)?.status)).forEach(r=>streamRun(r,epoch));
  }catch(error){if(epoch===caseEpoch){notice(error.message);$('#agent-state').textContent='Status unavailable · refresh';if($('#conversation').querySelector('.case-skeleton'))$('#conversation').innerHTML=`<div class="error">${esc(error.message)}</div>`;}}
}
function renderProposal() {
  const node=$('#proposal'),wasHidden=node.hidden;node.hidden=!proposal;if(!proposal)return;
  if(wasHidden)enter(node);
  const conflict=proposal.case_version!==current.version;
  node.innerHTML=`<h3>${esc(actions[proposal.action]||proposal.action)}</h3>${proposal.origin==='agent'?'':`<p>${esc(proposal.rationale)}</p>`}${groupsMarkup(proposal.groups)}<p class="caption">${proposal.origin==='human'?'Manual proposal':'Agent recommendation'} · case version ${proposal.case_version}${conflict?' · stale, refresh required':''}</p><p class="caption product-line"><img src="/assets/brand/unity-catalog-icon-full-color.svg" alt="">Confirmed decisions publish to Gold in Unity Catalog.</p><div class="proposal-actions"><button id="confirm-proposal" class="primary" ${busy||conflict?'disabled':''}>Confirm ${esc(label(proposal.action))}</button><button id="dismiss-proposal">Dismiss</button></div>`;
  $('#confirm-proposal').onclick=confirmProposal;
  if(proposal.golden_fields?.length){const summary=document.createElement('details');summary.className='evidence-pair';summary.innerHTML=`<summary>Selected golden fields</summary>${proposal.golden_fields.map((choices,g)=>`<h4>Guest ${g+1}</h4>${Object.entries(choices).map(([field,key])=>{const r=current.records.find(r=>r.record_key===key);return `<p>${esc(FIELDS.find(([,f])=>f===field)?.[0]||field)}: ${esc(r?.[field]??'Not provided')} <small>· ${esc(recordLabel(key))}</small></p>`}).join('')}`).join('')}`;node.querySelector('.proposal-actions').before(summary);}
  $('#dismiss-proposal').onclick=()=>{proposal=null;renderProposal();};
  if(proposal.origin==='agent') {
    if(wasHidden){const answers=$('#conversation').querySelectorAll('.answer'),last=answers[answers.length-1];if(last)requestAnimationFrame(()=>$('#conversation').scrollTo({top:Math.max(0,last.offsetTop-$('#conversation').offsetTop-96),behavior:calm()?'auto':'smooth'}));}
    $('#recommended').textContent='Agent recommends '+label(proposal.action);
    document.querySelectorAll('[data-action]').forEach(b=>b.classList.toggle('recommended',b.dataset.action===proposal.action));
  }
}
async function confirmProposal() {
  if(!proposal||busy)return; const displayed=proposal, epoch=caseEpoch;
  busy=true;controls();renderProposal();
  displayed.request_id ||= crypto.randomUUID();
  try {
    await api(`/api/proposals/${encodeURIComponent(displayed.proposal_id)}/confirm`,{request_id:displayed.request_id,confirmed_digest:displayed.confirmation_digest});
    notice('Decision saved.');if(epoch===caseEpoch)await loadCase(selected);await Promise.all([loadQueue(),loadSummary()]);
  }catch(error){notice(error.message);}finally{busy=false;controls();renderProposal();}
}
$('#chat').onsubmit=async event=>{
  event.preventDefault();if(!current||busy||$('#mode').value==='manual')return;
  const epoch=caseEpoch, message=$('#message').value.trim()||'Assess the source records independently. Explain the likely real-world situation and recommend guest groupings.';
  const body={case_version:current.version,mode:$('#mode').value,message};
  const signature=JSON.stringify([selected,body]);
  if(pendingSubmit?.signature!==signature)pendingSubmit={signature,request_id:crypto.randomUUID()};
  body.request_id=pendingSubmit.request_id;busy=true;controls();
  try {
    await api(`/api/cases/${encodeURIComponent(selected)}/agent-runs`,body);
    pendingSubmit=null;$('#message').value='';if(epoch===caseEpoch)await loadCase(selected,{soft:true});
  }catch(error){notice(error.message);}finally{busy=false;controls();}
};
document.querySelectorAll('[data-action]').forEach(button=>button.onclick=()=>{
  manualAction=button.dataset.action;proposal=null;renderProposal();$('#mode').value='manual';
  document.querySelectorAll('[data-action]').forEach(b=>b.setAttribute('aria-pressed',b===button));
  $('#partition').hidden=true;
  if(manualAction==='MERGE'||manualAction==='SEPARATE')guestEditor.arrange(manualAction);
  else if(manualAction==='PARTITION'){manualAction=guestEditor.draft.action();notice('Drag source records between Guest folders to define the groups.');$('#evidence').scrollTop=0;}
  controls();
});
$('#rationale').oninput=controls;
$('#preview-manual').onclick=async()=>{
  if(!current||!manualAction||busy)return; const epoch=caseEpoch;
  const groups=manualAction==='DEFER'?current.current_groups:guestEditor.draft.groups;
  const body={case_version:current.version,evidence_fingerprint:current.evidence_fingerprint,action:manualAction,groups,golden_fields:manualAction==='DEFER'?[]:guestEditor.draft.choices(),rationale:$('#rationale').value.trim(),evidence_refs:[]};
  busy=true;controls();try{const saved=await api(`/api/cases/${encodeURIComponent(selected)}/proposals`,body);if(epoch===caseEpoch){proposal={...body,...saved,origin:'human'};renderProposal();}}catch(error){notice(error.message);}finally{busy=false;controls();renderProposal();}
};
$('#mode').onchange=controls;
$('#status').onchange=$('#sort').onchange=()=>{offset=0;loadQueue();};
let searchTimer;
$('#search').oninput=()=>{clearTimeout(searchTimer);++queueEpoch;searchTimer=setTimeout(()=>{offset=0;loadQueue();},250);};
$('#previous').onclick=()=>{offset=Math.max(0,offset-PAGE);loadQueue();};
$('#next').onclick=()=>{offset+=PAGE;loadQueue();};
document.querySelectorAll('[data-lean]').forEach(button=>button.onclick=()=>{leanFilter=button.dataset.lean;offset=0;document.querySelectorAll('[data-lean]').forEach(b=>b.setAttribute('aria-checked',String(b===button)));loadQueue();});
$('#refresh').onclick=()=>{loadQueue();loadSummary();if(selected)loadCase(selected);};

function installResizers() {
  const work=$('#workspace'),panes=[...document.querySelectorAll('.pane')],minimums=[180,300,340];
  let ratios;
  try{const saved=JSON.parse(localStorage.getItem('steward-pane-ratios'));if(Array.isArray(saved)&&saved.length===3&&saved.every(n=>typeof n==='number'&&n>0&&n<1))ratios=saved;}catch{}
  function apply(sizes){work.style.gridTemplateColumns=`${sizes[0]}px 5px ${sizes[1]}px 5px ${sizes[2]}px`;document.querySelectorAll('.divider').forEach((bar,i)=>{bar.setAttribute('aria-valuemin',minimums[i]);bar.setAttribute('aria-valuemax',Math.round(sizes[i]+sizes[i+1]-minimums[i+1]));bar.setAttribute('aria-valuenow',Math.round(sizes[i]));});}
  function restore(){const width=work.clientWidth-10;if(ratios){const free=Math.max(0,width-minimums.reduce((a,b)=>a+b,0));const total=ratios.reduce((a,b)=>a+b,0);apply(minimums.map((m,i)=>m+free*ratios[i]/total));}}
  restore();
  document.querySelectorAll('.divider').forEach((bar,index)=>{
    function resize(delta,initial){const change=Math.max(minimums[index]-initial[index],Math.min(delta,initial[index+1]-minimums[index+1]));const sizes=[...initial];sizes[index]+=change;sizes[index+1]-=change;apply(sizes);const surplus=sizes.map((n,i)=>Math.max(0,n-minimums[i]));const total=surplus.reduce((a,b)=>a+b,0);ratios=total?surplus.map(n=>Math.max(.0001,n/total)):[1/3,1/3,1/3];try{localStorage.setItem('steward-pane-ratios',JSON.stringify(ratios));}catch{}}
    bar.onpointerdown=event=>{event.preventDefault();bar.setPointerCapture(event.pointerId);const start=event.clientX,sizes=panes.map(p=>p.getBoundingClientRect().width);document.body.classList.add('resizing');bar.onpointermove=e=>resize(e.clientX-start,sizes);const stop=()=>{bar.onpointermove=null;document.body.classList.remove('resizing');};bar.onpointerup=bar.onpointercancel=bar.onlostpointercapture=stop;};
    bar.onkeydown=event=>{if(['ArrowLeft','ArrowRight'].includes(event.key)){event.preventDefault();resize((event.key==='ArrowLeft'?-1:1)*(event.shiftKey?40:16),panes.map(p=>p.getBoundingClientRect().width));}};
  });
  window.addEventListener('resize',restore);
}
installResizers();controls();loadQueue();loadSummary();
api('/api/whoami').then(me=>{$('#identity').textContent=me.email;$('#identity').title=`Signed in as ${me.email}`;}).catch(error=>notice(error.message));
