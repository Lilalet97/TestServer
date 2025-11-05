// server.js (Node 18+)
const http = require('http');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('ok');               // 헬스체크용
});

const wss = new WebSocketServer({ server });

const workers = new Map();     // workerId -> { ws, tags:Set, max, running }
const wsRole  = new Map();     // ws -> { role:'worker'|'sender', id?:string }
const jobQueue = [];           // [{ jobId, fromWs, a, b }]
const jobToSender = new Map(); // jobId -> fromWs

function send(ws, obj){ if(ws && ws.readyState===ws.OPEN) ws.send(JSON.stringify(obj)); }

function pickWorker(){
  const cands = [];
  for (const [id, w] of workers){
    if (!w.ws || w.ws.readyState !== w.ws.OPEN) continue;
    if (!w.tags.has('calc')) continue;
    if (w.running >= w.max) continue;
    cands.push({ id, w });
  }
  if (cands.length===0) return null;
  cands.sort((A,B)=>(A.w.running/A.w.max)-(B.w.running/B.w.max));
  return cands[0];
}

function tryDispatch(){
  while(jobQueue.length){
    const pick = pickWorker();
    if(!pick) break;
    const job = jobQueue.shift();
    pick.w.running += 1;
    send(pick.w.ws, { v:1, type:'calc.assign', job_id:job.jobId, op:'add', a:job.a, b:job.b });
    console.log(`[assign] ${job.jobId} -> ${pick.id} (${pick.w.running}/${pick.w.max})`);
  }
}

function handleMessage(ws, raw){
  let msg; try{ msg = JSON.parse(raw.toString()); } catch{ return; }

  if (msg.type === 'worker.register'){
    const id = msg.worker_id || uuidv4();
    wsRole.set(ws, { role:'worker', id });
    workers.set(id, { ws, tags:new Set(msg.tags||[]), max: msg.max_concurrent||1, running:0, lastSeen:Date.now() });
    send(ws, { v:1, type:'worker.registered', worker_id:id });
    console.log(`[reg] worker=${id} tags=[${[...(msg.tags||[])].join(',')}] max=${msg.max_concurrent||1}`);
    tryDispatch(); return;
  }

  if (msg.type === 'worker.status'){
    const info = wsRole.get(ws); if(!info || info.role!=='worker') return;
    const w = workers.get(info.id); if(!w) return;
    if (typeof msg.running === 'number') w.running = Math.max(0, msg.running);
    w.lastSeen = Date.now(); tryDispatch(); return;
  }

  if (msg.type === 'calc.add'){
    wsRole.set(ws, { role:'sender' });
    const jobId = msg.job_id || uuidv4();
    const a = Number(msg.a), b = Number(msg.b);
    if (!Number.isFinite(a) || !Number.isFinite(b)){
      send(ws, { v:1, type:'error', code:'bad_args', detail:'a,b must be numbers' }); return;
    }
    jobQueue.push({ jobId, fromWs: ws, a, b });
    jobToSender.set(jobId, ws);
    console.log(`[enqueue] job=${jobId} a=${a} b=${b} queue=${jobQueue.length}`);
    tryDispatch(); return;
  }

  if (msg.type === 'calc.done'){
    const info = wsRole.get(ws); if(!info || info.role!=='worker') return;
    const jobId = msg.job_id, senderWs = jobToSender.get(jobId);
    if (senderWs) { send(senderWs, { v:1, type:'calc.done', job_id:jobId, result: msg.result, from_worker: info.id }); jobToSender.delete(jobId); }
    const w = workers.get(info.id); if (w) w.running = Math.max(0, w.running - 1);
    console.log(`[done] job=${jobId} result=${msg.result} worker=${info.id}`);
    tryDispatch(); return;
  }
}

wss.on('connection', (ws)=>{ ws.on('message', (d)=>handleMessage(ws, d)); });
server.listen(PORT, ()=>console.log(`Dispatcher on :${PORT}`));
