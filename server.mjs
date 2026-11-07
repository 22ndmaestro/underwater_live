import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { TikTokLiveClient, EventType, GiftStreakTracker } from 'piratetok-live-js';
const root=path.dirname(fileURLToPath(import.meta.url));
const PORT=Number(process.env.PORT||8787);
const server=http.createServer((req,res)=>{
 if(req.url==='/health'){res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({ok:true,service:'underwater-live-relay',node:process.version}));return}
 if(req.url==='/'||req.url==='/underwater_live_v12.html'){
  res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
  fs.createReadStream(path.join(root,'underwater_live_v12.html')).pipe(res);
 }else{res.writeHead(404);res.end('Not found')}
});
const wss=new WebSocketServer({server,path:'/events'});
const cleanUser=u=>({name:String(u?.nickname||u?.uniqueId||'Viewer').slice(0,80),userId:String(u?.userId||u?.id||u?.uniqueId||''),avatar:typeof(u?.avatarUrl)==='string'?u.avatarUrl:(u?.avatarThumb?.urlList?.[0]||u?.avatarMedium?.urlList?.[0]||'')});
wss.on('connection',socket=>{
 let client=null,token=0;
 const send=data=>{if(socket.readyState===1)socket.send(JSON.stringify(data))};
 const status=(state,message)=>send({type:'status',state,message});
 const stop=()=>{token++;if(client){try{client.disconnect()}catch{}client=null}};
 socket.on('message',async raw=>{
  let msg;try{msg=JSON.parse(raw.toString())}catch{return}
  if(msg.action==='disconnect'){stop();status('disconnected','Disconnected');return}
  if(msg.action!=='connect')return;
  const username=String(msg.username||'').replace(/^@/,'');
  if(!/^[A-Za-z0-9._]{2,30}$/.test(username)){status('error','Invalid username');return}
  stop();const current=token;const tracker=new GiftStreakTracker();
  const instance=new TikTokLiveClient(username).timeout(15000).maxRetries(5);client=instance;
  status('connecting','Connecting to @'+username+'…');
  const emit=(event,data,extra={})=>send({type:'event',event,...cleanUser(data?.user),...extra});
  instance.on(EventType.chat,d=>emit('comment',d));
  instance.on(EventType.like,d=>emit('like',d,{count:Math.max(1,Math.min(5000,Number(d.count)||1))}));
  instance.on(EventType.share,d=>emit('share',d));
  instance.on(EventType.gift,d=>{
   // GiftStreakTracker returns event delta, preventing cumulative streak double counting.
   const result=tracker.process(d);
   const diamonds=Math.max(0,Number(result?.totalDiamondCount)||0);
   if(diamonds>0)emit('gift',d,{diamonds,giftName:d.gift?.name||''});
  });
  instance.on(EventType.roomUserSeq,d=>send({type:'viewerCount',count:Number(d.viewerCount??d.viewer_count??0)||0}));
  const describe = value => {
    if (typeof value === 'string') return value;
    if (value instanceof Error) return value.message;
    if (value && typeof value === 'object') return String(value.reason?.message||value.reason||value.message||value.code||JSON.stringify(value));
    return String(value||'Unknown cause');
  };
  instance.on('reconnecting',info=>{
    if(current!==token)return;
    const reason=describe(info?.reason);
    const attempt=info?.attempt != null ? ` (attempt ${info.attempt}${info?.maxRetries?' of '+info.maxRetries:''})` : '';
    const message=`Reconnecting${attempt}: ${reason}`.slice(0,350);
    console.warn(`[PirateTok @${username}] ${message}`);
    status('reconnecting',message);
  });
  instance.on('error',error=>{
    if(current!==token)return;
    const reason=describe(error);
    console.error(`[PirateTok @${username}] Error: ${reason}`);
    status('error',`PirateTok error: ${reason}`.slice(0,350));
  });
  instance.on('connected',info=>{
    if(current!==token)return;
    console.log(`[PirateTok @${username}] Connected`,info||'');
    status('connected',`Connected to @${username}`);
  });
  instance.on('disconnected',info=>{if(current===token){const reason=describe(info?.reason||info);console.warn(`[PirateTok @${username}] Disconnected: ${reason}`);status('disconnected',`TikTok LIVE disconnected: ${reason}`.slice(0,350))}});
  try{const roomId=await instance.connect();if(current!==token){instance.disconnect();return}status('connected','Connected to @'+username+' · room '+roomId)}
  catch(e){if(current===token){console.error(`[PirateTok @${username}] Connection failed`,e);status('error','Connection failed: '+String(e.message||e).slice(0,300))}}
 });
 socket.on('close',stop);
});
server.listen(PORT,()=>console.log(`Underwater LIVE: http://localhost:${PORT}`));
