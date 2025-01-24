--2.3 dm.account_balance_turnover
-- прототип витрины
SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
-- 10575 строк

--1)Подготовить запрос, который определит корректное значение поля account_in_sum. 
--Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, 
--то корректным выбирается значение account_out_sum предыдущего дня.
with acc_balance_turn as (
SELECT ab.account_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
	   , coalesce(lag(ab.account_out_sum) over 
	   		(partition by ab.account_rk 
	   		order by ab.effective_date), 0) as day_before_account_out_sum
FROM rd.account_balance ab
)
select 
	abt.account_rk
	, abt.effective_date
	, case
		when abt.account_in_sum != abt.day_before_account_out_sum
		then abt.day_before_account_out_sum
		else abt.account_in_sum end as account_in_sum
	, abt.account_out_sum
from
	acc_balance_turn as abt;


--2)Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная, 
--а account_out_sum предыдущего дня некорректна. Это означает, что если эти значения отличаются, 
--то корректным значением для account_out_sum предыдущего дня выбирается значение account_in_sum текущего дня.
with acc_balance_turn as (
SELECT ab.account_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
	   , lead(ab.account_in_sum) over 
	   		(partition by ab.account_rk 
	   		order by ab.effective_date) as day_after_account_in_sum
FROM rd.account_balance ab
)
select 
	abt.account_rk
	, abt.effective_date
	, abt.account_in_sum 
	, case
		when abt.account_out_sum != abt.day_after_account_in_sum
		then abt.day_after_account_in_sum
		else abt.account_out_sum end as account_out_sum
from
	acc_balance_turn as abt;


--3)Подготовить запрос, который поправит данные в таблице rd.account_balance используя 
--уже имеющийся запрос из п.1
begin;
with acc_balance_turn as (
SELECT ab.account_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
	   , coalesce(lag(ab.account_out_sum) over 
	   		(partition by ab.account_rk 
	   		order by ab.effective_date), 0) as day_before_account_out_sum
FROM rd.account_balance ab
)
update
	rd.account_balance as ab
set
	account_in_sum = ab2.account_in_sum
from (
select 
	abt.account_rk
	, abt.effective_date
	, case
		when abt.account_in_sum != abt.day_before_account_out_sum
		then abt.day_before_account_out_sum
		else abt.account_in_sum end as account_in_sum
	, abt.account_out_sum
from
	acc_balance_turn as abt
) as ab2
where
	ab.account_rk = ab2.account_rk and ab.effective_date = ab2.effective_date;
--rollback;
commit;


--4)Написать процедуру по аналогии с задание 2.2 для перезагрузки данных в витрину
create or replace procedure dm.fill_account_balance_turnover()
language plpgsql as $$
begin
truncate dm.account_balance_turnover;
insert into dm.account_balance_turnover(
	account_rk
	, currency_name
	, department_rk
	, effective_date
	, account_in_sum
	, account_out_sum
)
SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
end;
$$;

-- поля account_in_sum
--Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, 
--то корректным выбирается значение account_out_sum предыдущего дня.
select * from dm.account_balance_turnover as abt where account_rk =  2943625;
-- до
--2943625	RUB	110	2023-01-28	0	25961
--2943625	RUB	110	2023-01-29	25961	85286
--2943625	RUB	110	2023-01-30	85286	*104895*
--2943625	RUB	110	2023-01-31	*104909* 115761
--2943625	RUB	110	2023-02-01	115761	176854
--2943625	RUB	110	2023-02-02	176854	227253

--после
--2943625	RUB	110	2023-01-28	0	25961
--2943625	RUB	110	2023-01-29	25961	85286
--2943625	RUB	110	2023-01-30	85286	*104895*
--2943625	RUB	110	2023-01-31	*104895* 115761
--2943625	RUB	110	2023-02-01	115761	176854
--2943625	RUB	110	2023-02-02	176854	227253

begin;
call dm.fill_account_balance_turnover();
rollback;
commit;



