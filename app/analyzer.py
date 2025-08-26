
from __future__ import annotations
import os, io, time, re, threading, logging, random
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
import requests
from http.cookiejar import MozillaCookieJar
import pandas as pd

log = logging.getLogger(__name__)

class TokenBucket:
    def __init__(self, rate_per_minute: int, burst: int = 3):
        self.capacity = max(1, burst); self.tokens = self.capacity
        self.rate = max(1, rate_per_minute)/60.0; self.last = time.perf_counter()
        self.lock = threading.Lock()
    def acquire(self, stop_event: Optional[threading.Event]=None):
        while True:
            if stop_event and stop_event.is_set(): raise StopIteration
            with self.lock:
                now = time.perf_counter(); dt = now - self.last; self.last = now
                self.tokens = min(self.capacity, self.tokens + dt*self.rate)
                if self.tokens >= 1: self.tokens -= 1; return
                need = (1 - self.tokens)/self.rate
            time.sleep(min(0.2, need))

@dataclass
class ClientConfig:
    base_url: str = 'https://www.avito.ru/'
    timeout: int = 25
    rate_per_min: int = 12
    burst: int = 3
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'

class AvitoClient:
    def __init__(self, cookies_path: Optional[str], cfg: ClientConfig):
        self.s = requests.Session(); self.cfg = cfg
        self.s.headers.update({'User-Agent': cfg.user_agent, 'Accept-Language':'ru-RU,ru;q=0.9'})
        self.bucket = TokenBucket(cfg.rate_per_min, cfg.burst)
        if cookies_path: self._load_cookies(cookies_path)
    def _load_cookies(self, path:str):
        try:
            jar = MozillaCookieJar(); jar.load(path, ignore_discard=True, ignore_expires=True)
            for c in jar: self.s.cookies.set_cookie(c)
            log.info('Loaded cookies: %s', path)
        except Exception as e: log.warning('Cookies load failed: %s', e)
    def get(self, url, params=None, stop_event=None):
        self.bucket.acquire(stop_event); return self.s.get(url, params=params, timeout=self.cfg.timeout, allow_redirects=True)

PRICE_RE = re.compile(r'(\d[\d\s]{2,}\s?[₽Рр])')
def parse_listing(html:str)->Dict[str,Any]:
    m = PRICE_RE.search(html); return {'found_price_text': m.group(1) if m else None}

@dataclass
class Item: idx:int; brand:str; model:str; buy_price:Optional[float]
def load_items_from_excel(path:str)->List[Item]:
    import io
    with open(path,'rb') as f: b=f.read()
    df=pd.read_excel(io.BytesIO(b))
    A=df.columns[0]; B=df.columns[1]; C=df.columns[2] if df.shape[1]>2 else None
    items=[]
    for i in range(len(df)):
        brand=str(df.iloc[i,0]) if pd.notna(df.iloc[i,0]) else ''
        model=str(df.iloc[i,1]) if pd.notna(df.iloc[i,1]) else ''
        bp=None
        if C is not None and pd.notna(df.iloc[i,2]): 
            try: bp=float(str(df.iloc[i,2]).replace(' ','').replace(',','.'))
            except: bp=None
        if brand or model: items.append(Item(i,brand,model,bp))
    return items

@dataclass
class Result: idx:int; query:str; ok:bool; data:Dict[str,Any]; http_status:Optional[int]=None; note:str=''

def respectful_sleep(s, stop_event=None):
    end=time.time()+s
    while time.time()<end:
        if stop_event and stop_event.is_set(): raise StopIteration
        time.sleep(0.1)

def has_captcha(t): return 'captcha' in t.lower() or 'капча' in t.lower()

def process_items(items:List[Item], client:AvitoClient, checkpoint='checkpoint.csv', stop_event=None, progress_cb=None)->List[Result]:
    res=[]; done=set()
    if os.path.exists(checkpoint):
        try:
            prev=pd.read_csv(checkpoint); done=set(prev['idx'].astype(int))
        except: pass
    total=len(items); processed=0; attempts={}
    for it in items:
        if stop_event and stop_event.is_set(): break
        if it.idx in done:
            processed+=1; progress_cb and progress_cb(processed,total,f'skip {it.idx}'); continue
        q=f'{it.brand} {it.model}'.strip()
        try:
            progress_cb and progress_cb(processed,total,f'GET {client.cfg.base_url} q={q}')
            r=client.get(client.cfg.base_url, params={'q':q}, stop_event=stop_event); st=r.status_code
            if st==200 and not has_captcha(r.text):
                data=parse_listing(r.text); res.append(Result(it.idx,q,True,data,200))
            else:
                res.append(Result(it.idx,q,False,{},st,'captcha' if st==200 else 'http'))
        except StopIteration: break
        except Exception as e:
            res.append(Result(it.idx,q,False,{},None,str(e)))
        processed+=1; progress_cb and progress_cb(processed,total,f'processed {it.idx}')
        if len(res)%5==0:
            _flush_checkpoint(res,checkpoint)
    _flush_checkpoint(res,checkpoint); return res

def _flush_checkpoint(results, path):
    import pandas as pd
    rows=[{'idx':r.idx,'query':r.query,'ok':r.ok,'http_status':r.http_status,'note':r.note,**{f'data_{k}':v for k,v in (r.data or {}).items()}} for r in results]
    pd.DataFrame(rows).to_csv(path,index=False,encoding='utf-8')

def dedupe_path(p):
    b,e=os.path.splitext(p); c=p; i=1
    while os.path.exists(c): c=f'{b} ({i}){e}'; i+=1
    return c

def save_output(results, src_excel, out_base=None)->Tuple[str,str]:
    import pandas as pd
    if out_base is None:
        stem=os.path.splitext(os.path.basename(src_excel))[0]; out_base=f'{stem}_analyzed'
    csv=dedupe_path(f'{out_base}.csv'); xlsx=dedupe_path(f'{out_base}.xlsx')
    rows=[{'idx':r.idx,'query':r.query,'ok':r.ok,'http_status':r.http_status,'note':r.note,**{f'data_{k}':v for k,v in (r.data or {}).items()}} for r in results]
    df=pd.DataFrame(rows); df.to_csv(csv,index=False,encoding='utf-8')
    with pd.ExcelWriter(xlsx,engine='openpyxl') as w: df.to_excel(w,index=False,sheet_name='Results')
    return csv,xlsx
