import React,{useRef,useState,useEffect} from 'react';

const price=x=>Number(x).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:Number(x)>=1000?2:Number(x)<1?8:6});
const stamp=t=>new Date(t*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
const clock=t=>new Date(t*1000).toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit'});
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const UP='#089981',DOWN='#d64b60';

export default function MarketChart({data,offline,focusPair}){
 const [pair,setPair]=useState('BTCUSDT'),[interval,setInterval]=useState('1m'),[count,setCount]=useState(80),[offset,setOffset]=useState(0),[hover,setHover]=useState(null);
 useEffect(()=>{if(focusPair){setPair(focusPair);setOffset(0)}},[focusPair]);
 const drag=useRef(null);
 const available=data.market_charts||[],pairs=[...new Set(available.map(c=>c.symbol))];
 const market=available.find(c=>c.symbol===pair&&c.interval===interval)||available.find(c=>c.symbol===pair)||available[0];
 const all=market?.candles||[];
 const end=all.length-clamp(offset,0,Math.max(0,all.length-20)),rows=all.slice(Math.max(0,end-count),end);
 const first=rows[0],last=rows.at(-1),live=all.at(-1),step=market?.interval_seconds||60;
 const quote=data.feed[market?.symbol];
 const stale=offline||!quote||quote.stale||market?.preview_stale||!data.signal_health?.find(h=>h.symbol===market?.symbol&&h.interval===market?.interval)?.fresh;
 const change=first&&last?(Number(last.close)/Number(first.open)-1)*100:0;
 const rangeStart=first?first.open_ms/1000:0,rangeEnd=last?last.open_ms/1000+step:1;
 const markers=(market?.markers||[]).filter(m=>m.at>=rangeStart&&m.at<rangeEnd);
 const levels=Object.entries(market?.levels||{}).map(([k,v])=>({name:({entry:'Entry',stop_loss:'Stop',take_profit:'Target'})[k],value:Number(v),color:k==='stop_loss'?DOWN:k==='take_profit'?UP:'#6381a2'}));
 const prices=rows.flatMap(r=>[Number(r.low),Number(r.high)]).concat(markers.map(m=>Number(m.price)),levels.map(l=>l.value));
 const lo=prices.length?Math.min(...prices):0,hi=prices.length?Math.max(...prices):1,pad=(hi-lo||Math.max(1,hi*.001))*.12;
 const bottom=lo-pad,top=hi+pad;
 const W=1000,H=410,left=14,right=906,yTop=20,yBottom=288,volTop=319,volBottom=375;
 const x=t=>left+(t-rangeStart)/Math.max(step,rangeEnd-rangeStart)*(right-left);
 const y=p=>yBottom-(p-bottom)/(top-bottom)*(yBottom-yTop);
 const candleWidth=clamp((right-left)*step/Math.max(step,rangeEnd-rangeStart)*.66,2,20);
 const maxVolume=Math.max(1,...rows.map(r=>Number(r.volume)));
 const selected=rows.find(r=>r.open_ms===hover?.time)||last;
 const selectedX=selected?x(selected.open_ms/1000+step/2):0;
 const lastColor=live&&Number(live.close)>=Number(live.open)?UP:DOWN;
 function move(e){
  const rect=e.currentTarget.getBoundingClientRect();
  if(drag.current){setOffset(clamp(drag.current.offset+Math.round((e.clientX-drag.current.x)/rect.width*rows.length),0,Math.max(0,all.length-20)));setHover(null);return}
  const px=(e.clientX-rect.left)/rect.width*W,py=(e.clientY-rect.top)/rect.height*H;
  const near=rows.reduce((a,b)=>!a||Math.abs(x(b.open_ms/1000+step/2)-px)<Math.abs(x(a.open_ms/1000+step/2)-px)?b:a,null);
  if(near)setHover({time:near.open_ms,y:clamp(py,yTop,yBottom)});
 }
 function chooseInterval(value){setInterval(value);setOffset(0);setHover(null)}
 return <section className="panel market-chart">
  <div className="market-chart-head"><div className="market-title"><div><h2>Market chart</h2><span className={'market-status '+(!stale?'online':'')}>{stale?'Price feed unavailable or stale':'Live public prices'}</span></div><div className="market-last" style={{color:lastColor}}>{live?price(live.close):'—'}<small>USDT · last candle price</small></div></div>
   <div className="chart-toolbar"><label className="sr-only" htmlFor="chart-market">Chart market</label><select id="chart-market" value={market?.symbol||pair} onChange={e=>{setPair(e.target.value);setOffset(0);setHover(null)}}>{pairs.length?pairs.map(s=><option key={s}>{s}</option>):<option>BTCUSDT</option>}</select><div className="timeframe-buttons" aria-label="Chart timeframe">{['1m','5m'].map(t=><button key={t} className={(market?.interval||interval)===t?'active':''} disabled={!available.some(c=>c.symbol===market?.symbol&&c.interval===t)} onClick={()=>chooseInterval(t)}>{t}</button>)}</div><span className={change>=0?'chart-up':'chart-down'}>{rows.length?(change>=0?'+':'')+change.toFixed(2)+'%':'—'} <small>visible range</small></span><div className="chart-zoom"><button aria-label="Zoom in chart" disabled={count<=20} onClick={()=>setCount(c=>Math.max(20,c-20))}>+</button><button aria-label="Zoom out chart" disabled={count>=200} onClick={()=>setCount(c=>Math.min(200,c+20))}>−</button><button onClick={()=>{setOffset(0);setHover(null)}}>Follow live</button></div></div>
  </div>
  {rows.length?<>
  <div className="chart-ohlc">{selected&&<><b>{stamp(selected.open_ms/1000)}</b>{['open','high','low','close'].map(k=><span key={k}>{k[0].toUpperCase()} <b>{price(selected[k])}</b></span>)}<span>Vol <b>{Number(selected.volume).toLocaleString(undefined,{maximumFractionDigits:4})}</b></span><em>{selected.complete?'Completed candle':selected.exchange_closed?'Exchange closed · awaiting local clock':'Forming candle · display only'}</em></>}</div>
  <div className="candlestick-wrap"><svg className="candlestick-svg" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`${market.symbol} ${market.interval} candlestick price and volume chart with recorded trade markers`} onPointerMove={move} onPointerDown={e=>{drag.current={x:e.clientX,offset};e.currentTarget.setPointerCapture(e.pointerId)}} onPointerUp={()=>{drag.current=null}} onPointerCancel={()=>{drag.current=null}} onPointerLeave={()=>{if(!drag.current)setHover(null)}}>
   <rect width={W} height={H} fill="#fff"/>
   {[0,1,2,3,4].map(i=>{const v=bottom+(top-bottom)*i/4,py=y(v);return <g key={i}><line x1={left} x2={right} y1={py} y2={py} stroke="#eaf0f2"/><text x={right+9} y={py+4} fill="#7c8c96" fontSize="11">{price(v)}</text></g>})}
   {[0,1,2,3,4].map(i=>{const t=rangeStart+(rangeEnd-rangeStart)*i/4,px=x(t);return <g key={i}><line x1={px} x2={px} y1={yTop} y2={volBottom} stroke="#f0f3f5"/><text x={px} y={H-10} textAnchor={i===0?'start':i===4?'end':'middle'} fill="#7c8c96" fontSize="11">{clock(t)}</text></g>})}
   <text x={left} y={volTop-5} fill="#91a0a8" fontSize="10">VOLUME</text>
   {rows.map(r=>{const o=Number(r.open),c=Number(r.close),px=x(r.open_ms/1000+step/2),color=c>=o?UP:DOWN;return <g key={r.open_ms} opacity={r.complete?1:.72}><line x1={px} x2={px} y1={y(Number(r.high))} y2={y(Number(r.low))} stroke={color} strokeWidth="1.2"/><rect x={px-candleWidth/2} y={Math.min(y(o),y(c))} width={candleWidth} height={Math.max(1,Math.abs(y(o)-y(c)))} fill={color}/><rect x={px-candleWidth/2} y={volBottom-Number(r.volume)/maxVolume*(volBottom-volTop)} width={candleWidth} height={Number(r.volume)/maxVolume*(volBottom-volTop)} fill={color} opacity=".3"/></g>})}
   {levels.map(l=><g key={l.name}><line x1={left} x2={right} y1={y(l.value)} y2={y(l.value)} stroke={l.color} strokeDasharray="5 4"/><text x={left+5} y={y(l.value)-5} fontSize="11" fill={l.color}>{data.execution_mode==='real'?'': 'Virtual'} {l.name} {price(l.value)}</text></g>)}
   {last&&<g><line x1={left} x2={right} y1={y(Number(last.close))} y2={y(Number(last.close))} stroke={Number(last.close)>=Number(last.open)?UP:DOWN} strokeDasharray="3 4" opacity=".7"/><rect x={right+1} y={y(Number(last.close))-10} width="88" height="20" rx="3" fill={Number(last.close)>=Number(last.open)?UP:DOWN}/><text x={right+7} y={y(Number(last.close))+4} fill="#fff" fontSize="11">{price(last.close)}</text></g>}
   {markers.map(m=>{const px=x(m.at),py=y(Number(m.price)),buy=m.side==='BUY',color=buy?UP:DOWN;return <g key={m.id}><title>{`${m.mode||'VIRTUAL'} ${m.side} · ${price(m.price)} USDT · ${m.quantity} units · ${stamp(m.at)} · ${m.reason}`}</title><circle cx={px} cy={py} r="4" fill="#fff" stroke={color} strokeWidth="2"/><path d={buy?`M ${px} ${py+7} l -5 9 h 10 z`:`M ${px} ${py-7} l -5 -9 h 10 z`} fill={color}/><text x={px} y={py+(buy?29:-21)} textAnchor="middle" fontWeight="700" fontSize="10" fill={color}>{buy?'BUY':'SELL'}</text></g>})}
   {hover&&selected&&<g pointerEvents="none"><line x1={selectedX} x2={selectedX} y1={yTop} y2={volBottom} stroke="#8799a4" strokeDasharray="4 4"/><line x1={left} x2={right} y1={hover.y} y2={hover.y} stroke="#8799a4" strokeDasharray="4 4"/><rect x={right+1} y={hover.y-10} width="88" height="20" fill="#526773" rx="3"/><text x={right+7} y={hover.y+4} fill="white" fontSize="11">{price(top-(hover.y-yTop)/(yBottom-yTop)*(top-bottom))}</text></g>}
  </svg></div>
  <div className="chart-navigation"><label htmlFor="chart-history">Browse earlier candles</label><input id="chart-history" type="range" min="0" max={Math.max(0,all.length-20)} value={clamp(offset,0,Math.max(0,all.length-20))} onChange={e=>{setOffset(Number(e.target.value));setHover(null)}}/><span>{offset===0?'Following latest':`${offset} bars back`} · {rows.length} candles</span></div>
  <div className="chart-footer"><span><i style={{background:UP}}/>Up candle <i style={{background:DOWN}}/>Down candle</span><span>{(market.markers||[]).length?`${markers.length} filled trade markers in this view`:'No fills in the selected wallet yet.'}</span><span>Hover for OHLC · drag to pan · +/− to zoom · local time</span></div>
  {stale&&<p className="chart-warning">Quotes are stale or unavailable. Displayed candles may be old.</p>}
  <details className="chart-data"><summary>Recent candle values and data source</summary><p>{market.source}. Forming candles never enter strategy decisions. Markers show recorded fills for the selected wallet; rejected proposals never become fills.</p><div className="table-scroll"><table><thead><tr>{['Time','Open','High','Low','Close','Volume','State'].map(h=><th key={h}>{h}</th>)}</tr></thead><tbody>{rows.slice(-10).map(r=><tr key={r.open_ms}><td>{stamp(r.open_ms/1000)}</td>{['open','high','low','close','volume'].map(k=><td key={k}>{price(r[k])}</td>)}<td>{r.complete?'Complete':'Display preview'}</td></tr>)}</tbody></table></div></details>
  </>:<p className="quiet-empty">Preparing public candles. Start the agent to connect and update the market chart.</p>}
 </section>;
}
