// Preserve existing DOM nodes (and their CSS animation timelines) during streaming.
export function patchChildren(parent, next) {
  const old = [...parent.childNodes];
  const keyed = new Map(old.filter(n=>n.nodeType===1 && n.dataset.renderKey)
    .map(n=>[n.dataset.renderKey,n]));
  const used = new Set();
  [...next.childNodes].forEach((fresh,index)=>{
    const key=fresh.nodeType===1?fresh.dataset.renderKey:null;
    let node=key?keyed.get(key):old[index];
    if(!node || used.has(node) || node.nodeType!==fresh.nodeType || node.nodeName!==fresh.nodeName ||
       (!key && node.nodeType===1 && node.dataset.renderKey)) node=fresh.cloneNode(true);
    else if(node.nodeType===3) { if(node.nodeValue!==fresh.nodeValue)node.nodeValue=fresh.nodeValue; }
    else if(node.nodeType===1) {
      for(const attr of [...node.attributes])if(!fresh.hasAttribute(attr.name) && !(node.tagName==='DETAILS' && attr.name==='open'))node.removeAttribute(attr.name);
      for(const attr of fresh.attributes)if(node.getAttribute(attr.name)!==attr.value)node.setAttribute(attr.name,attr.value);
      patchChildren(node,fresh);
    }
    used.add(node);
    if(parent.childNodes[index]!==node)parent.insertBefore(node,parent.childNodes[index]||null);
  });
  for(const node of old)if(!used.has(node))node.remove();
}
