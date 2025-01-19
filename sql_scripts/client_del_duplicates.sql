-- УДАЛЕНИЕ ДУБЛИКАТОВ В dm.client
select count(1) from dm.client as c ; -- до удаленгия дубликатов 20147, 3299 после

begin;
with dup_t as (
	select 
		row_number () over (partition by client_rk, effective_from_date order by effective_to_date desc) as row_num
		, client_rk , effective_from_date 
	from dm.client as c
)
delete 
	from
		dm.client as c2 
	where 
		(c2.client_rk, c2.effective_from_date) in (
		select d.client_rk, d.effective_from_date 
		from dup_t d
		where row_num > 1);
rollback; -- на всякий случай оставил возможность откатиться
end;