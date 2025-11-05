// server.js (Render/WebSocket) — txt2img 추가 버전
const http = require('http');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('ok'); // 헬스체크
});
const wss = new WebSocketServer({ server });

/** 상태 */
const workers = new Map();     // workerId -> { ws, tags:Set, max, running }
const wsRole  = new Map();     // ws -> { role:'worker'|'sender', id?:string }
const calcQueue = [];          // [{ jobId, fromWs, a, b }]
const imgQueue  = [];          // [{ jobId, fromWs, prompt, w,h,seed,steps }]

const jobToSender = new Map(); // jobId -> fromWs

function send(ws, obj){ if(ws && ws.readyState===ws.OPEN) ws.send(JSON.stringify(obj)); }

/** 워커 선택자 */
function pickWorkerByTag(tag){
  const cands = [];
  for (const [id, w] of workers){
    if (!w.ws || w.ws.readyState !== w.ws.OPEN) continue;
    if (!w.tags.has(tag)) continue;
    if (w.running >= w.max) continue;
    cands.push({ id, w });
  }
  if (cands.length===0) return null;
  cands.sort((A,B)=>(A.w.running/A.w.max)-(B.w.running/B.w.max)); // 최소부하
  return cands[0];
}

/** 디스패치 */
function tryDispatch(){
  // calc 먼저
  while(calcQueue.length){
    const pick = pickWorkerByTag('calc');
    if(!pick) break;
    const job = calcQueue.shift();
    pick.w.running += 1;
    send(pick.w.ws, { v:1, type:'calc.assign', job_id:job.jobId, op:'add', a:job.a, b:job.b });
    console.log(`[assign-calc] ${job.jobId} -> ${pick.id} (${pick.w.running}/${pick.w.max})`);
  }
  // txt2img
  while(imgQueue.length){
    const pick = pickWorkerByTag('comfy'); // ★ txt2img는 comfy 태그 워커로
    if(!pick) break;
    const job = imgQueue.shift();
    pick.w.running += 1;
    send(pick.w.ws, {
      v:1, type:'txt2img.assign', job_id:job.jobId,
      prompt: job.prompt, w: job.w, h: job.h, seed: job.seed, steps: job.steps
    });
    console.log(`[assign-img] ${job.jobId} -> ${pick.id} (${pick.w.running}/${pick.w.max})`);
  }
}

/** 메시지 처리 */
function handleMessage(ws, raw){
  let msg; try{ msg = JSON.parse(raw.toString()); } catch{ return; }

  // 워커 등록/상태
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

  // === calc (종전 테스트) ===
  if (msg.type === 'calc.add'){
    wsRole.set(ws, { role:'sender' });
    const jobId = msg.job_id || uuidv4();
    const a = Number(msg.a), b = Number(msg.b);
    if (!Number.isFinite(a) || !Number.isFinite(b)){
      send(ws, { v:1, type:'error', code:'bad_args', detail:'a,b must be numbers' }); return;
    }
    calcQueue.push({ jobId, fromWs: ws, a, b });
    jobToSender.set(jobId, ws);
    console.log(`[enqueue-calc] job=${jobId} a=${a} b=${b} q=${calcQueue.length}`);
    tryDispatch(); return;
  }
  if (msg.type === 'calc.done'){
    const info = wsRole.get(ws); if(!info || info.role!=='worker') return;
    const jobId = msg.job_id, senderWs = jobToSender.get(jobId);
    if (senderWs){ send(senderWs, { v:1, type:'calc.done', job_id:jobId, result: msg.result, from_worker: info.id }); jobToSender.delete(jobId); }
    const w = workers.get(info.id); if (w) w.running = Math.max(0, w.running - 1);
    console.log(`[done-calc] job=${jobId} res=${msg.result} worker=${info.id}`);
    tryDispatch(); return;
  }

  // === txt2img 새 기능 ===
  if (msg.type === 'txt2img.submit'){
    wsRole.set(ws, { role:'sender' });
    const jobId = msg.job_id || uuidv4();
    const prompt = String(msg.prompt || '');
    const w = Number(msg.w || 512), h = Number(msg.h || 512);
    const seed = Number.isFinite(Number(msg.seed)) ? Number(msg.seed) : Math.floor(Math.random()*1e9);
    const steps = Number(msg.steps || 20);
    imgQueue.push({ jobId, fromWs: ws, prompt, w, h, seed, steps });
    jobToSender.set(jobId, ws);
    console.log(`[enqueue-img] job=${jobId} prompt="${prompt}" ${w}x${h} q=${imgQueue.length}`);
    tryDispatch(); return;
  }
  if (msg.type === 'txt2img.done'){
    const info = wsRole.get(ws); if(!info || info.role!=='worker') return;
    const jobId = msg.job_id, senderWs = jobToSender.get(jobId);

    if (senderWs){
      send(senderWs, {
        v: 1,
        type: 'txt2img.done',
        job_id: jobId,
        image_b64: msg.image_b64,         // 성공 시 포함
        mime: msg.mime || 'image/png',
        error: msg.error,                  // ★ 에러도 함께 보냄
        detail: msg.detail,                // ★ 스택트레이스 등 상세
        from_worker: info.id
      });
      jobToSender.delete(jobId);
    }

    const w = workers.get(info.id);
    if (w) w.running = Math.max(0, w.running - 1);

    console.log(
      `[done-img] job=${jobId} size=${msg.image_b64 ? msg.image_b64.length : 0}` +
      (msg.error ? ` ERROR=${msg.error}` : '')
    );
    tryDispatch(); return;
  }
}

wss.on('connection', (ws)=>{ ws.on('message', (d)=>handleMessage(ws, d)); ws.on('close', ()=>{ const r=wsRole.get(ws); wsRole.delete(ws); if(r?.role==='worker'&&r.id){ workers.delete(r.id); console.log(`[close] worker=${r.id}`); } }); });
server.listen(PORT, ()=>console.log(`Dispatcher on :${PORT}`));
