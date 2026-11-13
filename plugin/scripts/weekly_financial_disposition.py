"""Quarantine a narrowly proven precision loss; never certify a revision"""
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

FIELDS={'现金及现金等价物','资产总额','负债总额','股东权益','总权益'}
METHOD='old_hk_annual_cny_precision_quarantine_v1'


def evidence(relative,field,previous,candidate,document,week_start):
    """Only existing prior-year CNY annual balance rows; no inferred availability"""
    if (relative.parts[:2]!=('cache','statements_hk') or field!='balance'
            or document.get('source')!='eastmoney_hk_statements' or len(previous)!=1):return None
    old=previous[0]
    try:
        period=date.fromisoformat(candidate['report_date'])
        if period>=week_start or period.year>=week_start.year:return None
        if candidate.get('report_type')!='年报' or candidate.get('currency')!='CNY':return None
        if candidate.get('unit_multiplier')!=1 or candidate.get('amount_unit')!='base_currency_unit':return None
        dates=('published_at','available_on','available_at','event_date','announcement_date','notice_date','publish_date')
        if any(old.get(k) or candidate.get(k) for k in dates):return None
        if {k:v for k,v in old.items() if k not in {'fields','fields_cny'}}!={k:v for k,v in candidate.items() if k not in {'fields','fields_cny'}}:return None
        if set(old['fields'])!=FIELDS or set(candidate['fields'])!=FIELDS:return None
        if old['fields']!=old['fields_cny'] or candidate['fields']!=candidate['fields_cny']:return None
        changes=[]
        for name,value in old['fields'].items():
            a,b=Decimal(str(value)),Decimal(str(candidate['fields'][name]))
            if not a.is_finite() or not b.is_finite():return None
            if a.quantize(Decimal('1E6'),rounding=ROUND_HALF_UP)!=b:return None
            if a!=b:changes.append({'field':name,'previous':value,'candidate':candidate['fields'][name]})
        if len(changes)<2:return None
        return {'method':METHOD,'source':document['source'],'report_period':period.isoformat(),
                'published_at':None,'classification':'undated','old_formal_values_retained':True,
                'formal_promotion_authorized':False,'correctness_of_candidate_certified':False,
                'changes':changes}
    except (KeyError,TypeError,ValueError,InvalidOperation):return None
