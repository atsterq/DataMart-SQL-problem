select count(1) from dm.account_balance_turnover as abt ;
select count(1) from dm.client as c ; -- 20147
select count(1) from dm.dict_currency as dc ;
select count(1) from dm.loan_holiday_info as lhi ; 


select client_rk, effective_from_date, count(*) as dup
from dm.client
group by client_rk, effective_from_date
having count(*) > 1;

