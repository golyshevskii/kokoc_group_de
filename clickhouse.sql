SHOW DATABASES;
/*
blogs
default
git_clickhouse
mgbench
system
*/

USE blogs;

select resort_name, state, 
count(resort_name) over (partition by state) as resort_count_per_state 
from blogs.resorts
group by resort_name, state
order by resort_count_per_state desc, resort_name;


with uppotp as (
    select street, price
    from blogs.uk_price_paid_oby_town_price
    where type in ('flat', 'terraced')
)
select uc.code, sum(uppotp.price) as total_price from uppotp
join blogs.uk_codes uc on uc.name = uppotp.street
group by 1
order by 2 desc;
