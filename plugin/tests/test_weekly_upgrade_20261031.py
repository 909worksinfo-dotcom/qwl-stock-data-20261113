"""Offline observed-failure regressions; no live collection or real publication"""
import copy
import hashlib
import importlib.util
import io
import json
import tarfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest
import requests
from scripts import weekly_guard as g
from scripts.weekly_runner import classify_task_failure, market_source_retry_evidence
from scripts.weekly_isolation import seed_announcement_days, IsolationSafetyError
from qwl.datasources import announcement_progress as notices, weekly_series, explicit_qfq
from qwl.schemas import Market

SKILL=Path('/Users/bytedance/.codex/skills/qwl-database-update-standard-workflow/scripts')


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,ensure_ascii=False))


def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def response(total=1,ids=('id',),code='688981'):
    data={'data':{'total_hits':total,'list':[{'art_code':name,'codes':[{'ann_type':'A','stock_code':code,'short_name':'name'}],
        'notice_date':'2026-10-30','title':'title','columns':[{'column_name':'公告'}]} for name in ids]}}
    raw=json.dumps(data).encode()
    return SimpleNamespace(json=lambda:copy.deepcopy(data),content=raw,url=notices.ENDPOINT,status_code=200,raise_for_status=lambda:None)


def test_notice_pages_duplicate_and_missing_are_not_complete(tmp_path):
    symbols={'688981':'688981.SH'};folder=tmp_path/'day';folder.mkdir()
    page=notices.parse(response(),'2026-10-30',1,symbols)
    write(folder/'one.json',page)
    receipt={'as_of':'2026-10-30','day':'2026-10-30','collector_sha256':notices.signature(),
        'pool_sha256':notices.digest(notices.canonical(symbols)),'total_hits':1,'selected_rows':1,
        'pages':[{'path':'one.json','sha256':notices.digest((folder/'one.json').read_bytes())}]}
    write(folder/'COMPLETE.json',receipt)
    assert len(notices.verify(folder,'2026-10-30','2026-10-30',symbols))==1
    receipt['total_hits']=101;write(folder/'COMPLETE.json',receipt)
    with pytest.raises(ValueError,match='page count'):notices.verify(folder,'2026-10-30','2026-10-30',symbols)
    receipt['total_hits']=1;write(folder/'COMPLETE.json',receipt);(folder/'one.json').write_bytes(b'drift')
    with pytest.raises(ValueError,match='SHA'):notices.verify(folder,'2026-10-30','2026-10-30',symbols)
    with pytest.raises(ValueError,match='duplicate'):notices.parse(response(total=2,ids=('id','id')),'2026-10-30',1,symbols)


def test_notice_empty_requires_official_zero_and_pool_is_filtered(tmp_path):
    symbols={'688981':'688981.SH'};folder=tmp_path/'day';folder.mkdir()
    receipt={'as_of':'2026-10-30','day':'2026-10-30','collector_sha256':notices.signature(),
        'pool_sha256':notices.digest(notices.canonical(symbols)),'total_hits':0,'selected_rows':0,'pages':[]}
    write(folder/'COMPLETE.json',receipt)
    with pytest.raises(ValueError,match='empty without'):notices.verify(folder,'2026-10-30','2026-10-30',symbols)
    receipt['verified_empty_probe_sha256']='a'*64;write(folder/'COMPLETE.json',receipt)
    assert notices.verify(folder,'2026-10-30','2026-10-30',symbols)==[]
    assert notices.parse(response(code='600000'),'2026-10-30',1,symbols)['rows']==[]
    with pytest.raises(ValueError,match='scope'):notices.verify(folder,'2026-11-06','2026-10-30',symbols)


def test_weekly_notice_retries_only_incomplete_day(monkeypatch,tmp_path):
    from qwl import paths,config
    ref=SimpleNamespace(symbol='688981.SH',ak_symbol='688981',market=Market.A)
    universe=SimpleNamespace(stocks=[ref]+[SimpleNamespace(symbol=str(i)+'.SZ',ak_symbol=str(i),market=Market.A) for i in range(59)])
    monkeypatch.setattr(config,'load_extended_universe',lambda:universe)
    monkeypatch.setattr(paths,'ROOT',tmp_path);monkeypatch.setattr(paths,'CACHE_DIR',tmp_path/'.cache')
    calls=[];failed=[True]
    def request(day,page):
        calls.append((day,page))
        if day=='2026-10-29' and failed[0]:raise requests.Timeout('synthetic transient')
        return response(total=0,ids=()),{}
    monkeypatch.setattr(notices,'request',request)
    a=notices.collect_week(universe,'2026-10-30',polite=0)
    assert a['status']=='PARTIAL' and a['incomplete_query_days']==['2026-10-29']
    failed[0]=False;calls.clear();b=notices.collect_week(universe,'2026-10-30',polite=0)
    assert b['status']=='OK' and calls==[('2026-10-29',1)] and len(b['verified_query_days'])==5


def test_announcement_archive_hash_drift_rejected(tmp_path):
    root=tmp_path/'attempts';receipt=root/'1/announcements_a.json';receipt.parent.mkdir(parents=True)
    archive=receipt.with_suffix('.tar.gz');name='state/snapshots/2026-10-30/announcement_daily_v2/day/proof.json'
    with tarfile.open(archive,'w:gz') as tar:
        m=tarfile.TarInfo(name);m.size=2;tar.addfile(m,io.BytesIO(b'{}'))
    write(receipt,{'provider_snapshot':str(archive),'result':{'announcement_archive':{'source_archive':str(archive.resolve()),'sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}}})
    workspace=tmp_path/'workspace';workspace.mkdir();seed_announcement_days(workspace,root/'2/announcements_a.json','2026-10-30')
    assert (workspace/name).exists()
    archive.write_bytes(archive.read_bytes()+b'drift')
    with pytest.raises(IsolationSafetyError,match='SHA'):seed_announcement_days(workspace,root/'2/announcements_a.json','2026-10-30')


def prices():
    f=pd.DataFrame({'open':10.,'high':11.,'low':9.,'close':10.,'volume':100.},index=pd.bdate_range(end='2026-10-30',periods=90))
    f.attrs['price_basis']='qfq';return f


def test_old_ohlc_anomaly_is_not_recaptured_or_certified(tmp_path):
    f=prices();f.iloc[0,f.columns.get_loc('low')]=12
    ref=SimpleNamespace(symbol='688981.SH',market=Market.A);calls=[]
    def fetch():calls.append(1);return f,'tencent_kline_qfq'
    a=weekly_series.collect(tmp_path,ref,date(2026,10,30),'qfq','sig',fetch)
    b=weekly_series.collect(tmp_path,ref,date(2026,10,30),'qfq','sig',fetch)
    assert len(calls)==1 and b[0].attrs['weekly_validation']['whole_series_valid'] is False
    assert b[0].iloc[0].low==12 and not weekly_series.validate(b[0],ref,date(2026,10,30),'qfq')


@pytest.mark.parametrize('defect',['current','anchor','missing_session','future','duplicate','nan','basis'])
def test_current_or_structural_source_defects_never_cached(tmp_path,defect):
    f=prices();ref=SimpleNamespace(symbol='688981.SH',market=Market.A)
    if defect=='current':f.iloc[-1,f.columns.get_loc('low')]=12
    if defect=='anchor':f.loc[pd.Timestamp('2026-10-23'),'low']=12
    if defect=='missing_session':f=f.drop(pd.Timestamp('2026-10-28'))
    if defect=='future':f.index=f.index+pd.Timedelta(days=7)
    if defect=='duplicate':f=pd.concat([f,f.iloc[-1:]])
    if defect=='nan':f.iloc[0,0]=float('nan')
    if defect=='basis':f.attrs['price_basis']='none'
    weekly_series.collect(tmp_path,ref,date(2026,10,30),'qfq','sig',lambda:(f,'tencent_kline_qfq'))
    assert not list(tmp_path.rglob('receipt.json'))


@pytest.mark.parametrize('rows',[[['2026-11-02',1]],[['1900-01-01',0]],[['1900-01-01',float('nan')]], [['1900-01-01',1],['1900-01-01',1]]])
def test_qfq_bad_or_future_factor_is_rejected(rows):
    with pytest.raises(ValueError):explicit_qfq.factors({'data':rows},'2026-10-30')


def test_sina_formula_date_and_volume_identity():
    f=prices().reset_index(names='date');raw=f.copy();factor=explicit_qfq.factors({'data':[['1900-01-01',1]]},'2026-10-30')
    assert explicit_qfq.normalize_sina(f,raw,factor,'2026-10-30').iloc[-1].volume==100
    bad=f.copy();bad['volume']/=100
    with pytest.raises(ValueError,match='volume'):explicit_qfq.normalize_sina(bad,raw,factor,'2026-10-30')
    bad=f.copy();bad['close']+=1
    with pytest.raises(ValueError,match='arithmetic'):explicit_qfq.normalize_sina(bad,raw,factor,'2026-10-30')
    raw['date']=raw.date.astype(str)+'T00:00:00.000Z'
    assert len(explicit_qfq.normalize_sina(f,raw,factor,'2026-10-30'))==90
    with pytest.raises((ValueError,SyntaxError)):explicit_qfq.safe_literal("__import__('os').system('false')")


def private_market(monkeypatch,tmp_path):
    from qwl import paths,config
    ref=next(r for r in config.load_extended_universe().stocks if r.symbol=='688981.SH')
    root=tmp_path/'qwl-item-test';root.mkdir()
    monkeypatch.setattr(paths,'ROOT',root);monkeypatch.setattr(paths,'CACHE_DIR',root/'.cache')
    raw=prices();raw.attrs['price_basis']='none'
    directory=root/'state/snapshots/2026-10-30/series/688981_SH/none';directory.mkdir(parents=True)
    raw.to_parquet(directory/'bars-raw.parquet')
    write(directory/'receipt.json',{'symbol':ref.symbol,'basis':'none','as_of':'2026-10-30',
        'source':'tencent_kline_raw','data_file':'bars-raw.parquet','sha256':explicit_qfq.sha(directory/'bars-raw.parquet')})
    formal=root/'state/history/688981_SH.parquet';formal.parent.mkdir(parents=True)
    raw.loc[raw.index<pd.Timestamp('2026-10-26')].to_parquet(formal)
    return root,ref,raw


def test_native_sina_three_wires_and_cache_proof_replay(monkeypatch,tmp_path):
    import akshare as ak
    root,ref,raw=private_market(monkeypatch,tmp_path)
    native=ak.stock_zh_a_daily;m=__import__(native.__module__,fromlist=['requests']);calls=[]
    decoded=raw.reset_index(names='date');decoded['date']=decoded.date.dt.date.astype(str);decoded['amount']=range(1000,1000+len(decoded))
    records=decoded.to_dict('records')
    class Decoder:
        def eval(self,code):pass
        def call(self,name,data):return copy.deepcopy(records)
    def get(url,**kwargs):
        kind=next(k for k,v in explicit_qfq.urls(ref).items() if explicit_qfq.same_url(url,v));calls.append(kind)
        text=('var data="COMPRESSED";' if kind=='history' else
              'var data='+json.dumps({'data':[['1900-01-01','1']]}) if kind=='factor' else
              'var amount='+json.dumps([{'date':records[0]['date'],'amount':'1000000'}]))
        return SimpleNamespace(text=text,content=text.encode(),url=url,status_code=200,raise_for_status=lambda:None)
    monkeypatch.setattr(m,'requests',SimpleNamespace(get=get));monkeypatch.setattr(m,'py_mini_racer',SimpleNamespace(MiniRacer=Decoder))
    directory=root/'state/snapshots/2026-10-30/series'
    def fetch():return explicit_qfq.fetch(ref,'20260101','20261030','sina'),'akshare_sina_a_qfq'
    a=weekly_series.collect(directory,ref,date(2026,10,30),'qfq','signature',fetch)
    b=weekly_series.collect(directory,ref,date(2026,10,30),'qfq','signature',fetch)
    assert calls==['history','amount','factor'] and b[0].iloc[-1].volume==100
    proof=json.loads(next((directory/'688981_SH/qfq').glob('*-proof.json')).read_text())
    bad=directory/'688981_SH/qfq'/proof['wire']['factor']['response_member'];bad.write_bytes(bad.read_bytes()+b'drift')
    with pytest.raises(ValueError,match='source SHA'):
        weekly_series.collect(directory,ref,date(2026,10,30),'qfq','signature',fetch)
    assert calls==['history','amount','factor']


@pytest.mark.parametrize('volume',[100,1])
def test_native_em_units_are_never_fitted(monkeypatch,tmp_path,volume):
    import akshare as ak
    root,ref,raw=private_market(monkeypatch,tmp_path)
    native=ak.stock_zh_a_hist;m=__import__(native.__module__,fromlist=['requests'])
    klines=[','.join([str(day.date()),'10','10','11','9',str(volume),'10000','0','0','0','0']) for day in raw.index]
    payload={'data':{'code':'688981','klines':klines}}
    def get(url,**kwargs):
        assert kwargs['params']['fqt']=='1'
        return SimpleNamespace(content=json.dumps(payload).encode(),json=lambda:payload,
            url=explicit_qfq.EM+'?fqt=1&secid=1.688981',status_code=200,raise_for_status=lambda:None)
    monkeypatch.setattr(m,'requests',SimpleNamespace(get=get))
    result=explicit_qfq.fetch(ref,'20260101','20261030','em')
    if volume==100:assert result.iloc[-1].volume==100 and result.attrs['price_basis']=='qfq'
    else:
        assert result is None
        assert list((root/'state/snapshots/2026-10-30/series/688981_SH/qfq').glob('*-unit-rejected.json'))


def test_actual_cross_week_pit_branch_has_json_output(monkeypatch,tmp_path,capsys):
    m=module('pit_actual_crossweek',SKILL/'publish_all12_pit_release.py')
    parent=tmp_path/'parent';parent.mkdir();write(parent/'release.json',{'week_end':'2026-10-23'})
    project=tmp_path/'project';project.mkdir();evidence=tmp_path/'evidence'
    args=SimpleNamespace(registry=str(project/'weekly_plugin_runner/registry.json'),as_of='2026-10-30',
        weekly_run_id='run',parent_release_sha='old-sha',release_id='new',evidence=str(evidence))
    monkeypatch.setattr(m,'validate_args',lambda args:(tmp_path/'db',project,parent,tmp_path/'target'))
    builder=SimpleNamespace(configure=lambda args:None,build=lambda:{'status':'PUBLISHED_ALL12_PIT_PARENT'})
    monkeypatch.setattr(m.importlib.util,'spec_from_file_location',lambda *args:SimpleNamespace(loader=SimpleNamespace(exec_module=lambda obj:None)))
    monkeypatch.setattr(m.importlib.util,'module_from_spec',lambda spec:builder)
    monkeypatch.setattr(m,'parser',lambda:SimpleNamespace(parse_args=lambda:args))
    assert m.main()==0
    assert json.loads(capsys.readouterr().out)['status']=='PUBLISHED_ALL12_PIT_PARENT'


def old_balance():
    fields={'现金及现金等价物':65424923000.,'资产总额':291204565000.,'负债总额':197405625000.,'股东权益':92582221000.,'总权益':93798940000.}
    return {'report_date':'2025-12-31','published_at':None,'currency':'CNY','report_type':'年报','unit_multiplier':1,
            'amount_unit':'base_currency_unit','fields':fields,'fields_cny':copy.deepcopy(fields)}


def financial_plan(tmp_path,old,new):
    rel=Path('cache/statements_hk/00175.json');db=tmp_path/'db';source=tmp_path/'candidate.json'
    write(db/rel,{'balance':[old]});write(source,{'source':'eastmoney_hk_statements','balance':[new]})
    return g._plan_json(source,db/rel,rel,db,date(2026,10,26),date(2026,10,30))


def rounded(old):
    from decimal import Decimal,ROUND_HALF_UP
    new=copy.deepcopy(old);new['fields']={k:float(Decimal(str(v)).quantize(Decimal('1E6'),rounding=ROUND_HALF_UP)) for k,v in old['fields'].items()};new['fields_cny']=copy.deepcopy(new['fields']);return new


def test_known_precision_disposition_is_quarantine_only(tmp_path):
    old=old_balance();new=rounded(old);changes=financial_plan(tmp_path,old,new)
    assert [x.kind for x in changes]==['undated_precision_quarantine']
    assert changes[0].payload['revisions'][0]['classification']=='undated'
    assert changes[0].payload['revisions'][0]['proof']['formal_promotion_authorized'] is False
    assert json.loads((tmp_path/'db/cache/statements_hk/00175.json').read_text())['balance']==[old]


@pytest.mark.parametrize('defect',['real_value','current_period','published','currency','unit','other_field'])
def test_unknown_or_current_financial_change_remains_blocking_revision(tmp_path,defect):
    old=old_balance();new=rounded(old)
    if defect=='real_value':new['fields']['资产总额']+=10;new['fields_cny']=copy.deepcopy(new['fields'])
    if defect=='current_period':old['report_date']=new['report_date']='2026-09-30'
    if defect=='published':new['published_at']='2026-10-29'
    if defect=='currency':new['currency']='HKD'
    if defect=='unit':new['unit_multiplier']=1000
    if defect=='other_field':new['accounting_standard']='changed'
    changes=financial_plan(tmp_path,old,new)
    assert not any(x.kind=='undated_precision_quarantine' for x in changes)
    # Currency/date are business-key changes and remain isolated, never appended
    assert not any(x.kind=='weekly_append' for x in changes)


def test_source_retry_does_not_accept_other_fatals_or_current_conflicts(tmp_path,monkeypatch):
    from qwl import config
    syms={'688981.SH'}|{str(i)+'.SZ' for i in range(59)}
    monkeypatch.setattr(config,'load_extended_universe',lambda:SimpleNamespace(stocks=[SimpleNamespace(symbol=s) for s in syms]))
    folder=tmp_path/'job';folder.mkdir();archive=folder/'market.tar.gz';receipt=folder/'market.json';prefix='state/snapshots/2026-10-30/id/'
    with tarfile.open(archive,'w:gz') as tar:
        for name,body in [(prefix+'meta.json',json.dumps({'symbols':{s:{} for s in syms}}).encode())]+[(prefix+kind+'/'+s.replace('.','_')+'.parquet',b'x') for s in syms for kind in ['ohlcv','ohlcv_standardized'] if not(s=='688981.SH' and kind=='ohlcv_standardized')]:
            m=tarfile.TarInfo(name);m.size=len(body);tar.addfile(m,io.BytesIO(body))
    result={'as_of':'2026-10-30','universe_size':60,'run_id':'id','gate':{'passed':False,'fatal_count':1,'issues':[{'severity':'fatal','locus':'688981.SH:ohlcv:none','message':'OHLCV missing/empty from all sources'}]},'history_sync':{'status':'SKIPPED_GATE_FAILED','symbols':{},'appended_rows':0,'conflict_rows':0,'price_basis_blocked':False}}
    write(receipt,{'ok':False,'result':result,'provider_snapshot':str(archive)})
    state={'tasks':{'market':{'ok':False,'receipt':str(receipt),'failure_class':'RETRY_SOURCE'}},'gaps':[]}
    proof=market_source_retry_evidence(state,folder,tmp_path,'2026-10-30');assert proof['retry_allowed'] and not proof['publication_allowed']
    state['gaps']=[{'blocking':True,'domain':'history','reason':'current_history_invalid','details':{'status':'conflict','snapshot_mismatch_dates':['2026-10-29']}}]
    assert market_source_retry_evidence(state,folder,tmp_path,'2026-10-30') is None
    state['gaps']=[];result['gate']['issues'][0]['message']='price conflict';write(receipt,{'ok':False,'result':result,'provider_snapshot':str(archive)})
    assert market_source_retry_evidence(state,folder,tmp_path,'2026-10-30') is None
    assert classify_task_failure({'ok':False,'result':{'error':'EXPLICIT_QFQ_INTEGRITY: price'}})=='BLOCKED_ITEM_SAFETY'


@pytest.mark.parametrize('parent_week',['2026-10-23','2026-10-30'])
def test_pit_cli_prints_exactly_one_json_for_both_branches(monkeypatch,capsys,parent_week):
    m=module('pit_cli_'+parent_week,SKILL/'publish_all12_pit_release.py')
    # build's contract is now pure-return in both real paths; CLI owns output
    monkeypatch.setattr(m,'build',lambda args:{'status':'PUBLISHED','branch':parent_week})
    monkeypatch.setattr(m,'parser',lambda:SimpleNamespace(parse_args=lambda:None))
    assert m.main()==0 and json.loads(capsys.readouterr().out)['branch']==parent_week


def test_committed_pit_output_loss_recovery_does_not_republish(monkeypatch,tmp_path):
    m=module('pipeline_recovery_20261031',SKILL/'run_weekly_end_to_end.py');release=tmp_path/'release';release.mkdir()
    write(release/'release.json',{'weekly_run_id':'run'})
    calls=[]
    def invoke(cmd):calls.append(cmd);raise m.CommandOutputError('empty')
    monkeypatch.setattr(m,'invoke',invoke);monkeypatch.setattr(m,'require_release',lambda p,asof:'sha')
    assert m.publish_or_recover(['cmd'],release,'2026-10-30','run')['release_sha256']=='sha' and len(calls)==1
    with pytest.raises(RuntimeError,match='RUN_DRIFT'):m.publish_or_recover(['cmd'],release,'2026-10-30','other')
    monkeypatch.setattr(m,'invoke',lambda cmd:(_ for _ in ()).throw(RuntimeError('safety blocked')))
    with pytest.raises(RuntimeError,match='safety blocked'):m.publish_or_recover(['cmd'],release,'2026-10-30','run')


def test_unpromoted_market_timeout_retry_is_hash_bound(tmp_path):
    folder=tmp_path/'job';folder.mkdir();archive=folder/'market.tar.gz';receipt=folder/'market.json'
    with tarfile.open(archive,'w:gz'):pass
    marker={'as_of':'2026-10-30','working_cache_promoted':False,'source_archive':str(archive.resolve()),
            'sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}
    result={'status':'timeout','error':'worker deadline/exit before durable receipt','market_partial_collection':marker}
    write(receipt,{'ok':False,'result':result,'provider_snapshot':str(archive)})
    state={'tasks':{'market':{'ok':False,'receipt':str(receipt),'failure_class':'RETRY_SOURCE'}},'gaps':[]}
    proof=market_source_retry_evidence(state,folder,tmp_path,'2026-10-30')
    assert proof['retry_allowed'] and proof['publication_allowed'] is False
    marker['working_cache_promoted']=True;write(receipt,{'ok':False,'result':result,'provider_snapshot':str(archive)})
    assert market_source_retry_evidence(state,folder,tmp_path,'2026-10-30') is None
    marker['working_cache_promoted']=False;marker['sha256']='0'*64;write(receipt,{'ok':False,'result':result,'provider_snapshot':str(archive)})
    assert market_source_retry_evidence(state,folder,tmp_path,'2026-10-30') is None


def test_precision_candidate_does_not_write_formal_values_on_publish(tmp_path):
    old=old_balance();new=rounded(old);rel=Path('cache/statements_hk/00175.json')
    db=tmp_path/'db';source=tmp_path/'source';write(db/rel,{'balance':[old]})
    write(source/'.cache/statements_hk/00175.json',{'source':'eastmoney_hk_statements','balance':[new]})
    sealed=tmp_path/'sealed';sealed.mkdir();(sealed/'x').write_text('frozen');freeze=tmp_path/'freeze.json'
    g.build_freeze_manifest([('sealed',sealed)],freeze)
    original=(db/rel).read_bytes();mtime=(db/rel).stat().st_mtime_ns
    result=g.publish_weekly(source,db,'2026-10-30',publish=True,authorization='PUBLISH_CURRENT_WEEK',
        freeze_manifest=freeze,allowed_stems={'00175'},acceptance=lambda *args:{'passed':True,'issues':[]})
    assert result['status']=='PUBLISHED' and result['historical_revisions_published']==0
    assert (db/rel).read_bytes()==original and (db/rel).stat().st_mtime_ns==mtime
    assert not list((db/'state/published_versions').rglob('*.json'))
    quarantine=list((db/'state/quarantine').rglob('undated_precision_quarantine.json'))
    assert len(quarantine)==1 and json.loads(quarantine[0].read_text())['revisions'][0]['candidate']==new
    assert result['formal_acceptance']['quarantined_financial_precision_candidates']


def test_current_ohlcv_fault_is_hard_but_old_anomaly_and_missing_date_are_not_repaired():
    ref=SimpleNamespace(symbol='688981.SH',market=Market.A);f=prices()
    f.iloc[0,f.columns.get_loc('low')]=12
    weekly_series.assert_current_integrity(f,ref,date(2026,10,30))
    assert f.iloc[0].low==12
    f=f.drop(pd.Timestamp('2026-10-28'))
    weekly_series.assert_current_integrity(f,ref,date(2026,10,30))
    assert weekly_series.weekly_validation(f,ref,date(2026,10,30),'qfq') is None
    f.iloc[-1,f.columns.get_loc('low')]=12
    with pytest.raises(ValueError,match='SERIES_INTEGRITY'):weekly_series.assert_current_integrity(f,ref,date(2026,10,30))


def test_strict_existing_release_contract_compatibility_and_manifest_tamper(monkeypatch,tmp_path):
    m=module('pipeline_manifest_20261031',SKILL/'run_weekly_end_to_end.py')
    project=tmp_path/'project';monkeypatch.setattr(m,'PROJECT',project)
    models=[{'symbol':str(i),'inputs':['input']} for i in range(12)];write(project/'weekly_plugin_runner/registry.json',{'models':models})
    release=tmp_path/'release';release.mkdir()
    write(release/'release.json',{'week_end':'2026-10-30','latest_trade_date':'2026-10-30','scope_symbols':[str(i) for i in range(12)],
        'scope_model_count':12,'scope_consumption_input_count':1,'logical_files':['input','inherited-baseline-extra'],'quarantine_members_included':0})
    fingerprint=m.sha(release/'release.json');write(release/'ENTRYPOINT.json',{'release_sha256':fingerprint})
    lines=[m.sha(release/name)+'  '+str((release/name).stat().st_size)+'  state/pit_gateway/releases/release/'+name for name in ['release.json','ENTRYPOINT.json']]
    (release/'MANIFEST_SHA256_SIZE.txt').write_text('\n'.join(lines)+'\n')
    write(release/'COMMIT.json',{'status':'ACCEPTED_MANUAL_RELEASE','private_candidate':False,'release_sha256':fingerprint,'manifest_sha256':m.sha(release/'MANIFEST_SHA256_SIZE.txt')})
    assert m.require_release(release,'2026-10-30')==fingerprint
    (release/'ENTRYPOINT.json').write_text('drift')
    with pytest.raises(RuntimeError,match='MEMBER_SHA'):m.require_release(release,'2026-10-30')
