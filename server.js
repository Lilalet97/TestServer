const http = require('http');
const { WebSocketServer, WebSocket } = require('ws'); // ← WebSocket.OPEN 사용
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 8080;

// 헬스체크 HTTP
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('ok');
});

// 대용량 Base64 전송 대비 maxPayload 상향
const wss = new WebSocketServer({
  server,
  maxPayload: 64 * 1024 * 1024, // 64MB
});

console.log(`Dispatcher on :${PORT}`);

// ===== 상태 저장소 =====
const workers = new Map();     // workerId -> { ws, tags:Set, max, running, lastSeen }
const wsRole  = new Map();     // ws -> { role:'worker'|'sender', id?:string }
const calcQueue = [];          // [{ jobId, fromWs, a, b }]
const imgQueue  = [];          // [{ jobId, fromWs, payload }]
const jobToSender = new Map(); // jobId -> fromWs

function send(ws, obj) {
  if (!ws) return;
  if (ws.readyState !== WebSocket.OPEN) return;  // ← 올바른 OPEN 상수로 체크
  ws.send(JSON.stringify(obj));
}

// 워커 선택(태그/최소부하)
function pickWorkerByTag(tag) {
  const cands = [];
  for (const [id, w] of workers) {
    if (!w.ws || w.ws.readyState !== WebSocket.OPEN) continue;
    if (tag && !w.tags.has(tag)) continue;
    if (w.running >= w.max) continue;
    cands.push({ id, w });
  }
  if (cands.length === 0) return null;
  cands.sort((A, B) => (A.w.running / A.w.max) - (B.w.running / B.w.max));
  return cands[0];
}

// 워커로 assign 시 payload에서 type/job_id/v 제거
function stripMetaFields(payload) {
  if (!payload || typeof payload !== 'object') return {};
  const { type, job_id, v, ...rest } = payload;
  return rest;
}

function tryDispatch() {
  // calc 먼저
  while (calcQueue.length) {
    const pick = pickWorkerByTag('calc');
    if (!pick) break;
    const job = calcQueue.shift();
    pick.w.running += 1;
    send(pick.w.ws, {
      v: 1,
      type: 'calc.assign',
      job_id: job.jobId,
      op: 'add',
      a: job.a,
      b: job.b,
    });
    console.log(`[assign-calc] ${job.jobId} -> ${pick.id} (${pick.w.running}/${pick.w.max})`);
  }

  // txt2img
  while (imgQueue.length) {
    const pick = pickWorkerByTag('comfy'); // comfy 태그 워커에게만
    if (!pick) break;
    const job = imgQueue.shift();
    pick.w.running += 1;

    const forwarded = stripMetaFields(job.payload); // 모든 옵션 pass-through
    send(pick.w.ws, {
      v: 1,
      type: 'txt2img.assign',
      job_id: job.jobId,
      ...forwarded, // prompt, w,h,seed,steps,cfg,sampler,scheduler,negative,fmt,quality,workflow,timeout 등
    });

    console.log(`[assign-img] ${job.jobId} -> ${pick.id} (${pick.w.running}/${pick.w.max})`);
  }
}

function handleMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw.toString()); } catch { return; }

  // ===== 워커 등록/상태 =====
  if (msg.type === 'worker.register') {
    const id = msg.worker_id || uuidv4();
    const max = Math.max(1, parseInt(msg.max_concurrent || 1, 10));
    const tags = new Set(Array.isArray(msg.tags) ? msg.tags : []);
    wsRole.set(ws, { role: 'worker', id });
    workers.set(id, { ws, tags, max, running: 0, lastSeen: Date.now() });
    send(ws, { v: 1, type: 'worker.registered', worker_id: id });
    console.log(`[reg] worker=${id} tags=[${[...tags].join(',')}] max=${max}`);
    tryDispatch(); return;
  }

  if (msg.type === 'worker.status') {
    const info = wsRole.get(ws); if (!info || info.role !== 'worker') return;
    const w = workers.get(info.id); if (!w) return;
    if (typeof msg.running === 'number') w.running = Math.max(0, msg.running);
    w.lastSeen = Date.now();
    tryDispatch(); return;
  }

  // ===== calc (기존 테스트 유지) =====
  if (msg.type === 'calc.add') {
    wsRole.set(ws, { role: 'sender' });
    const jobId = msg.job_id || uuidv4();
    const a = Number(msg.a), b = Number(msg.b);
    if (!Number.isFinite(a) || !Number.isFinite(b)) {
      send(ws, { v: 1, type: 'error', code: 'bad_args', detail: 'a,b must be numbers' });
      return;
    }
    calcQueue.push({ jobId, fromWs: ws, a, b });
    jobToSender.set(jobId, ws);
    console.log(`[enqueue-calc] job=${jobId} a=${a} b=${b} q=${calcQueue.length}`);
    tryDispatch(); return;
  }

  if (msg.type === 'calc.done') {
    const info = wsRole.get(ws); if (!info || info.role !== 'worker') return;
    const jobId = msg.job_id, senderWs = jobToSender.get(jobId);
    if (senderWs) {
      send(senderWs, { v: 1, type: 'calc.done', job_id: jobId, result: msg.result, from_worker: info.id });
      jobToSender.delete(jobId);
    }
    const w = workers.get(info.id); if (w) w.running = Math.max(0, w.running - 1);
    console.log(`[done-calc] job=${jobId} res=${msg.result} worker=${info.id}`);
    tryDispatch(); return;
  }

  // ===== txt2img (신규) =====
  if (msg.type === 'txt2img.submit') {
    wsRole.set(ws, { role: 'sender' });
    const jobId = msg.job_id || uuidv4();

    // 원본 payload를 통째로 보관 → 워커에 그대로 전달(위 stripMetaFields로 type/job_id/v만 제거)
    imgQueue.push({ jobId, fromWs: ws, payload: msg });
    jobToSender.set(jobId, ws);

    const p = String(msg.prompt || '');
    const w = Number(msg.w || 512), h = Number(msg.h || 512);
    console.log(`[enqueue-img] job=${jobId} prompt="${p}" ${w}x${h} q=${imgQueue.length}`);

    tryDispatch(); return;
  }

  if (msg.type === 'txt2img.done') {
    const info = wsRole.get(ws); 
    if (!info || info.role !== 'worker') return;

    const jobId = msg.job_id;
    const senderWs = jobToSender.get(jobId);

    if (senderWs) {
      send(senderWs, {
        v: 1,
        type: 'txt2img.done',
        job_id: jobId,
        image_b64: msg.image_b64,                  // (이미지 b64도 여전히 지원)
        mime: msg.mime || 'image/png',
        result_file_url: msg.result_file_url || null,  // ★ 추가
        media_type: msg.media_type || null,            // ★ 선택: image / video 구분용
        error: msg.error,
        detail: msg.detail,
        from_worker: info.id
      });
      jobToSender.delete(jobId);
    }

    const w = workers.get(info.id); 
    if (w) w.running = Math.max(0, w.running - 1);

    const size = msg.image_b64 ? msg.image_b64.length : 0;
    const url  = msg.result_file_url || '';
    console.log(
      `[done-img] job=${jobId} size=${size} url=${url}` +
      (msg.error ? ` ERROR=${msg.error}` : '')
    );

    tryDispatch(); 
    return;
  }
}

// 연결 수명주기
wss.on('connection', (ws) => {
  ws.on('message', (d) => handleMessage(ws, d));
  ws.on('close', () => {
    const r = wsRole.get(ws);
    wsRole.delete(ws);
    if (r?.role === 'worker' && r.id) {
      workers.delete(r.id);
      console.log(`[close] worker=${r.id}`);
    }
  });
});

// 시작
server.listen(PORT, () => console.log(`Dispatcher on :${PORT}`));
