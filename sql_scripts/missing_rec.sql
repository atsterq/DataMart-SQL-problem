-- 2.2
-- анализ данных
select -- пропуски дат в rd.deal 
    'rd.deal' as table_name
    , count(1) filter (where effective_from_date is null) as missing_from_date
    , count(1) filter (where effective_to_date is null) as missing_to_date
from rd.deal_info as di 
union
select -- пропуски дат в rd.product
    'rd.product' as table_name
    , count(1) filter (where effective_from_date is null) as missing_from_date
    , count(1) filter (where effective_to_date is null) as missing_to_date
from rd.product
union
select -- пропуски дат в rd.loan_holiday
    'rd.loan_holiday' as table_name
    , count(1) filter (where effective_from_date is null) as missing_from_date
    , count(1) filter (where effective_to_date is null) as missing_to_date
from rd.loan_holiday
union
select -- пропуски дат в dm.loan_holiday_info
    'dm.loan_holiday_info' as table_name
    , count(1) filter (where effective_from_date is null) as missing_from_date
    , count(1) filter (where effective_to_date is null) as missing_to_date
from dm.loan_holiday_info
union
select -- пропуски дат в data.deal_info
    'data.deal_info' as table_name
    , count(1) filter (where effective_from_date is null) as missing_from_date
    , count(1) filter (where effective_to_date is null) as missing_to_date
from data.deal_info
union
select -- пропуски дат в data.product_info
    'data.product_info' as table_name
    , count(1) filter (where effective_from_date is null) as missing_from_date
    , count(1) filter (where effective_to_date is null) as missing_to_date
from data.product_info
order by table_name;
-- пропусков нет


-- сколько записей всего
select 
	'rd.deal_info' as table_name --6500
	,( select count(1) from rd.deal_info as di3)
union
select 
	'rd.product' as table_name --3500
	,( select count(1) from rd.product as di3)
union
select 
	'rd.loan_holiday' as table_name --10000
	,( select count(1) from rd.loan_holiday as di3)
union
select 
	'dm.loan_holiday_info' as table_name --10002
	,( select count(1) from dm.loan_holiday_info)
union
select 
	'data.deal_info' as table_name --3500
	,( select count(1) from data.deal_info as di3)
union
select 
	'data.product_info' as table_name --10000
	,( select count(1) from data.product_info as di3)
order by table_name;


-- дубли в dm.loan_holiday_info
select lhi.deal_rk , lhi.effective_from_date , count(1)
from dm.loan_holiday_info as lhi 
group by deal_rk
  	, effective_from_date
having count(1) > 1;
--4531242	2023-08-11	4
--2594431	2023-03-15	2


-- дубли в rd.loan_holiday
select deal_rk, effective_from_date, count(1)
from rd.loan_holiday as lh 
group by deal_rk, effective_from_date
having count(1) > 1;
--4531242	2023-08-11	2
--2594431	2023-03-15	2


-- дубли в rd.deal_info
select deal_rk, effective_from_date, count(1)
from rd.deal_info as di 
group by deal_rk, effective_from_date
having count(1) > 1;
--4531242	2023-08-11	2


-- дубли в rd.product
select product_rk , effective_from_date, count(1)
from rd.product as p 
group by product_rk , effective_from_date
having count(1) > 1;
--1668282	2023-03-15	2
--1956251	2023-03-15	2
--1979096	2023-03-15	2
--1308366	2023-03-15	2


-- совпадения deal
select count(1) as deal_info_intersect from ( 
select * from rd.deal_info as di 
intersect
select * from data.deal_info as di2) t ;
-- полных совпадений нет


-- совпадения product
select count(1) as product_intersect from (
select * from rd.product as p 
intersect
select * from data.product_info as pi2) t ;
 -- 3498 совпадений


-- все данные product
select t.*
from (
select 
	p.product_rk as data_product_rk, p.product_name as data_product_name, p.effective_from_date data_from_date, p.effective_to_date as data_to_date
	,rd.product_rk as rd_product_rk, rd.product_name as rd_product_name, rd.effective_from_date rd_from_date, rd.effective_to_date as rd_to_date
from data.product_info p
full join rd.product rd 
    on p.product_rk = rd.product_rk 
--    and p.effective_from_date = rd.effective_from_date
--where rd.product_rk is null
) t
where 
	(data_product_rk is null or data_product_name is null or data_from_date is null or data_to_date is null)
	or (rd_product_rk is null or rd_product_name is null or rd_from_date is null or rd_to_date is null);


-- даты в витрине dm.loan_holiday_info
select effective_from_date , effective_to_date 
from dm.loan_holiday_info
group by effective_from_date , effective_to_date ;
--2023-03-15	2999-12-31
--2023-08-11	2999-12-31
--2023-01-01	2999-12-31


-- даты в rd.loan_holiday
select effective_from_date , effective_to_date 
from rd.loan_holiday
group by effective_from_date , effective_to_date ;
--2023-03-15	2999-12-31
--2023-08-11	2999-12-31
--2023-01-01	2999-12-31


-- какие даты product есть в rd слое
select effective_from_date , effective_to_date 
from rd.product as p 
group by effective_from_date , effective_to_date ;
-- 2023-03-15	2999-12-31


-- какие даты product есть в выгрузке
select effective_from_date , effective_to_date 
from data.product_info as pi2 
group by effective_from_date , effective_to_date ;
--2023-03-15	2999-12-31
--2023-08-11	2999-12-31 - нет в rd слое
--2023-01-01	2999-12-31 - нет в rd слое


-- даты deal в rd
select effective_from_date , effective_to_date 
from rd.deal_info as di 
group by effective_from_date , effective_to_date ;
--2023-08-11	2999-12-31
--2023-01-01	2999-12-31


-- даты deal в выгрузке
select effective_from_date , effective_to_date 
from data.deal_info as di 
group by effective_from_date , effective_to_date ;
--2023-03-15	2999-12-31 - нет в rd слое


-- вывод: 
-- строки в построенной витрине dm.loan_holiday_info отсутсвуют по датам эффективности effective_from_date
-- в таблицу rd.product необходимо загрузить данные за 2023-08-11, 2023-01-01
-- в таблицу rd.deal_info необходимо дозагрузить полную таблицу из выгрузки к уже имеющимся данным
-- построить витрину исходя из дополненных данных
-- по задаче не ясно, нужно что-то делать c дублями или нет (можно решить добавлением distinct в заполнение витрины)
--------------


-- дополним данные в rd слое
begin;

-- в product обновить существующие даннык
with updated_data as (
    select 
		pi.product_rk
        , pi.effective_from_date
        , pi.product_name
        , pi.effective_to_date
    from 
    	data.product_info pi
    where exists (
        select 1
        from rd.product p
        where 
        	pi.product_rk = p.product_rk 
            and pi.effective_from_date = p.effective_from_date)
)
update rd.product r
set 
	product_name = u.product_name
    , effective_to_date = u.effective_to_date
from updated_data u
where 
	r.product_rk = u.product_rk 
	and r.effective_from_date = u.effective_from_date;

-- в product загрузить отсутствующие данные
insert into 
	rd.product
select 
	p.product_rk
	, p.product_name
    , p.effective_from_date
    , p.effective_to_date
from 
	data.product_info p
where not exists (
    select 1
    from rd.product r
    where 
		r.product_rk = p.product_rk 
		and r.effective_from_date = p.effective_from_date
);

select count(1) from rd.product as p ; -- 10000


-- в deal дозагрузим всю таблицу
insert into rd.deal_info
select 
	di.*
from 
	data.deal_info di;

select count(1) from rd.deal_info as di ; -- 10000


-- удалить дубликаты можно таким запросом
--select 
--	min(ctid)
--from data.product_info
--group by 
--	product_rk
--	, effective_from_date;

commit;
rollback;
--------------
-- процедура для наполнения витрины dm.loan_holiday_info
-- оставим старую витрину, для новой создадим таблицу dm.loan_holiday_info_v2
create table if not exists dm.loan_holiday_info_v2(
	deal_rk bigint not null
	, effective_from_date date not null
	, effective_to_date date not null
	, agreement_rk bigint
	, client_rk bigint
	, department_rk bigint
	, product_rk bigint
	, product_name text
	, deal_type_cd text
	, deal_start_date date
	, deal_name text
	, deal_number text
	, deal_sum numeric
	, loan_holiday_type_cd text
	, loan_holiday_start_date date
	, loan_holiday_finish_date date
	, loan_holiday_fact_finish_date date
	, loan_holiday_finish_flg boolean
	, loan_holiday_last_possible_date date
);


create or replace procedure dm.fill_loan_holiday_info_v2()
language plpgsql as $$
begin
truncate dm.loan_holiday_info_v2;
insert into dm.loan_holiday_info_v2(
	deal_rk
	, effective_from_date
	, effective_to_date
	, agreement_rk
	, client_rk
	, department_rk
	, product_rk
	, product_name 
	, deal_type_cd 
	, deal_start_date
	, deal_name 
	, deal_number
	, deal_sum 
	, loan_holiday_type_cd 
	, loan_holiday_start_date 
	, loan_holiday_finish_date
	, loan_holiday_fact_finish_date 
	, loan_holiday_finish_flg 
	, loan_holiday_last_possible_date 
)
with deal as (
select  deal_rk
	   ,deal_num --Номер сделки
	   ,deal_name --Наименование сделки
	   ,deal_sum --Сумма сделки
	   ,client_rk --Ссылка на клиента
	   ,agreement_rk --Ссылка на договор
	   ,deal_start_date --Дата начала действия сделки
	   ,department_rk --Ссылка на отделение
	   ,product_rk -- Ссылка на продукт
	   ,deal_type_cd
	   ,effective_from_date
	   ,effective_to_date
from RD.deal_info
), loan_holiday as (
select  deal_rk
	   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
	   ,loan_holiday_start_date     --Дата начала кредитных каникул
	   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
	   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
	   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
	   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
	   ,effective_from_date
	   ,effective_to_date
from RD.loan_holiday
), product as (
select product_rk
	  ,product_name
	  ,effective_from_date
	  ,effective_to_date
from RD.product
), holiday_info as (
select   d.deal_rk
        ,lh.effective_from_date
        ,lh.effective_to_date
        ,d.deal_num as deal_number --Номер сделки
	    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
        ,d.deal_name --Наименование сделки
        ,d.deal_sum --Сумма сделки
        ,d.client_rk --Ссылка на контрагента
        ,d.agreement_rk --Ссылка на договор
        ,d.deal_start_date --Дата начала действия сделки
        ,d.department_rk --Ссылка на ГО/филиал
        ,d.product_rk -- Ссылка на продукт
        ,p.product_name -- Наименование продукта
        ,d.deal_type_cd -- Наименование типа сделки
from deal d
left join loan_holiday lh on 1=1
                             and d.deal_rk = lh.deal_rk
                             and d.effective_from_date = lh.effective_from_date
left join product p on p.product_rk = d.product_rk
					   and p.effective_from_date = d.effective_from_date
)
SELECT deal_rk
      ,effective_from_date
      ,effective_to_date
      ,agreement_rk
      ,client_rk
      ,department_rk
      ,product_rk
      ,product_name
      ,deal_type_cd
      ,deal_start_date
      ,deal_name
      ,deal_number
      ,deal_sum
      ,loan_holiday_type_cd
      ,loan_holiday_start_date
      ,loan_holiday_finish_date
      ,loan_holiday_fact_finish_date
      ,loan_holiday_finish_flg
      ,loan_holiday_last_possible_date
FROM holiday_info;
end;
$$;

call dm.fill_loan_holiday_info_v2();

select count(1) from dm.loan_holiday_info_v2 as lhiv ; --10040


-- без дублей (при помощи distinct на исходных таблицах, уменьшает производительность)
create table if not exists dm.loan_holiday_info_v3(
	deal_rk bigint not null
	, effective_from_date date not null
	, effective_to_date date not null
	, agreement_rk bigint
	, client_rk bigint
	, department_rk bigint
	, product_rk bigint
	, product_name text
	, deal_type_cd text
	, deal_start_date date
	, deal_name text
	, deal_number text
	, deal_sum numeric
	, loan_holiday_type_cd text
	, loan_holiday_start_date date
	, loan_holiday_finish_date date
	, loan_holiday_fact_finish_date date
	, loan_holiday_finish_flg boolean
	, loan_holiday_last_possible_date date
);


create or replace procedure dm.fill_loan_holiday_info_v3()
language plpgsql as $$
begin
truncate dm.loan_holiday_info_v3;
insert into dm.loan_holiday_info_v3(
	deal_rk
	, effective_from_date
	, effective_to_date
	, agreement_rk
	, client_rk
	, department_rk
	, product_rk
	, product_name 
	, deal_type_cd 
	, deal_start_date
	, deal_name 
	, deal_number
	, deal_sum 
	, loan_holiday_type_cd 
	, loan_holiday_start_date 
	, loan_holiday_finish_date
	, loan_holiday_fact_finish_date 
	, loan_holiday_finish_flg 
	, loan_holiday_last_possible_date 
)
with deal as (
select  distinct on (deal_rk, effective_from_date)
		deal_rk
	   ,deal_num --Номер сделки
	   ,deal_name --Наименование сделки
	   ,deal_sum --Сумма сделки
	   ,client_rk --Ссылка на клиента
	   ,agreement_rk --Ссылка на договор
	   ,deal_start_date --Дата начала действия сделки
	   ,department_rk --Ссылка на отделение
	   ,product_rk -- Ссылка на продукт
	   ,deal_type_cd
	   ,effective_from_date
	   ,effective_to_date
from RD.deal_info
), loan_holiday as (
select  distinct on (deal_rk, effective_from_date)
		deal_rk
	   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
	   ,loan_holiday_start_date     --Дата начала кредитных каникул
	   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
	   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
	   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
	   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
	   ,effective_from_date
	   ,effective_to_date
from RD.loan_holiday
), product as (
select distinct on (product_rk, effective_from_date)
		product_rk
	  ,product_name
	  ,effective_from_date
	  ,effective_to_date
from RD.product
), holiday_info as (
select   d.deal_rk
        ,lh.effective_from_date
        ,lh.effective_to_date
        ,d.deal_num as deal_number --Номер сделки
	    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
        ,d.deal_name --Наименование сделки
        ,d.deal_sum --Сумма сделки
        ,d.client_rk --Ссылка на контрагента
        ,d.agreement_rk --Ссылка на договор
        ,d.deal_start_date --Дата начала действия сделки
        ,d.department_rk --Ссылка на ГО/филиал
        ,d.product_rk -- Ссылка на продукт
        ,p.product_name -- Наименование продукта
        ,d.deal_type_cd -- Наименование типа сделки
from deal d
left join loan_holiday lh on 1=1
                             and d.deal_rk = lh.deal_rk
                             and d.effective_from_date = lh.effective_from_date
left join product p on p.product_rk = d.product_rk
					   and p.effective_from_date = d.effective_from_date
)
SELECT deal_rk
      ,effective_from_date
      ,effective_to_date
      ,agreement_rk
      ,client_rk
      ,department_rk
      ,product_rk
      ,product_name
      ,deal_type_cd
      ,deal_start_date
      ,deal_name
      ,deal_number
      ,deal_sum
      ,loan_holiday_type_cd
      ,loan_holiday_start_date
      ,loan_holiday_finish_date
      ,loan_holiday_fact_finish_date
      ,loan_holiday_finish_flg
      ,loan_holiday_last_possible_date
FROM holiday_info;
end;
$$;

call dm.fill_loan_holiday_info_v3();

select count(1) from dm.loan_holiday_info_v3 as lhiv3 ; --9998


-- при сложных вычислениях и больших данных можно удалить дубли в исходных таблицах 

