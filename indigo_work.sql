create table indigo.fips_control as 
select sf.fips as state_fips, sf."name" as state_name, sf."postal code" as state_abbr, cc.*
from indigo.state_fips sf 
inner join indigo.county_conus cc on sf.fips = cc.statefp 

create table indigo.corn_soybean_production_2018_2020 as 
select year, concat('0', "state ansi" ) as state_fips, "county ansi" as county_fips, commodity , value as bushels
from indigo.nass_corn_production_2018_2020 where length("state ansi"::text) < 2
and county not like 'OTHER %' 
union 
select year, "state ansi" as state_fips, "county ansi" as county_fips, commodity , value as bushels 
from indigo.nass_corn_production_2018_2020 cp where length("state ansi"::text) = 2 
and county not like 'OTHER %'
union
select year, concat('0', "state ansi" ) as state_fips, "county ansi" as county_fips, commodity , value as bushels
from indigo.nass_soybean_production_2018_2020 sp where length("state ansi"::text) < 2 
and county not like 'OTHER %'
union
select year, "state ansi" as state_fips, "county ansi" as county_fips, commodity , value as bushels 
from indigo.nass_soybean_production_2018_2020 sp where length("state ansi"::text) = 2 
and county not like 'OTHER %'
order by 2, 3, 1, 4

create index crop_three_year_average_2018_2020_2_sidx
on indigo.crop_three_year_average_2018_2020_2 using GIST(geom)

create index corn_soybean_production_2018_2020_full_sidx
on indigo.corn_soybean_production_2018_2020_full using GIST(geom)

create table indigo.corn_soybean_production_2018_2020_summary as  
select distinct county_or_equivalent, state_name, county_fips , year,
case when commodity = 'CORN' and bushels > 0 then bushels else 0 end as corn, 
case when commodity = 'SOYBEANS' and bushels > 0 then bushels else 0 end as soybeans 
from indigo.corn_soybean_production_2018_2020_full cspf 
order by 2,1,3

select county_or_equivalent, state_name, county_fips ,
case when commodity = 'CORN' then avg(bushels) end as corn, 
case when commodity = 'SOYBEANS' then avg(bushels) end as soybeans 
from indigo.corn_soybean_production_2018_2020_full cspf 
group by county_or_equivalent, state_name , county_fips , commodity 
order by 2,1,3

SELECT county_fips, AVG(bushels) as corn_three_yr_avg
FROM indigo.corn_soybean_production_2018_2020_full cspf 
where commodity = 'CORN'
GROUP BY county_fips order by county_fips 

--noticed a decent range so maybe an average of non-census years would be more accurate
create table indigo.crop_three_year_average as 
select concat(fc.statefp, fc.countyfp) as fips,  
       round(sum(case when t.commodity = 'CORN' then t.bushels end) / 3, 2) as corn,
       round(sum(case when t.commodity = 'SOYBEANS' then t.bushels end) / 3, 2) as soybeans
from indigo.fips_control fc 
left join indigo.corn_soybean_production_2018_2020_full t ON (fc.statefp || countyfp = t.county_fips)
where commodity is not null
group by fips
order by fips

create table indigo.crop_2020 as
select year, state_fips , county_fips , commodity , 
replace(bushels, ',','') as bushels
from indigo.corn_soybean_production_2018_2020 
where year = '2020'

--noticed a decent range so maybe an average of non-census years would be more accurate
create table indigo.crop_three_year_average_2018_2020_2 as 

select cc.statefp || cc.countyfp as fips, cc.namelsad as county_or_equivalent,
       round(sum(case when t.commodity = 'CORN' then t.bushels end) / 3, 2) as corn_bushels,
       round(sum(case when t.commodity = 'SOYBEANS' then t.bushels end) / 3, 2) as soybean_bushels,
cc.geom       
from indigo.county_conus cc 
left join indigo.corn_soybean_production_2018_2020_2 t ON (cc.statefp || cc.countyfp = t.state_fips || county_fips)
where commodity is not null
group by fips, cc.namelsad , cc.geom 
order by fips

create table indigo.crop_2020_2 as 
select c.state_fips ,c.county_fips, c.year,
sum(case when c.commodity = 'CORN' then c.bushels end) as corn_bushels,
sum(case when c.commodity = 'SOYBEANS' then c.bushels end) as soybean_bushels
from indigo.crop_2020 c 
group by state_fips, county_fips , year
order by 1,2

update indigo.crop_2020_2
set corn_bushels = 0 where corn_bushels is null

update indigo.crop_2020_2
set soybean_bushels = 0 where soybean_bushels is null

create table indigo.crop_three_year_average_2018_2020_3 as
select c.* , corn_bushels + soybean_bushels as crop_total_bushels
from indigo.crop_2020_2 c 

select a.fips , a.county_or_equivalent, sf."name" as state, a.corn_bushels , a.soybean_bushels , a.geom 
from indigo.crop_three_year_average_2018_2020_2 a
left join indigo.state_fips sf on substring(a.fips,1,2) = sf.fips 
order by state 

select distinct substring(fips,1,2) as state 
from indigo.crop_three_year_average_2018_2020_2 ctya 
order by state

--final 202 crop table
create table indigo.crop_2020_final_2 as 
select c.fips, corn_bushels , soybean_bushels , corn_bushels + soybean_bushels as crop_total_bushels,
cc."name" as county , sf."name" as state, cc.geom 
from indigo.crop_2020_final c 
left join indigo.county_conus cc on c.fips = cc.statefp || cc.countyfp 
left join indigo.state_fips sf on cc.statefp = sf.fips 
order by state 

create index crop_2020_final_sidx
on indigo.crop_2020_final using GIST(geom)

create table indigo.buyers_final_work as
select b.id, cc.countyfp , cc.statefp ,cc."name" as county, b.geom 
from indigo.indigo_case_study_500_random_buyers b
join indigo.county_conus cc on ST_Contains(cc.geom, b.geom)

create table indigo.buyers_final as 
select b.id , crop.county, crop.state , 
crop.crop_total_bushels as crop_total_bushels_2020, crop.corn_bushels as corn_bushels_2020 , 
crop.soybean_bushels as soybean_bushels_2020, b.geom 
from indigo.indigo_case_study_500_random_buyers b
join indigo.crop_2020_final as crop on ST_Contains(crop.geom, b.geom)

select count(*) from indigo.indigo_case_study_500_random_buyers icsrb 

--8 buyers outside US
select b.id , b.geom 
from indigo.indigo_case_study_500_random_buyers b
left join indigo.county_conus cc  on ST_Intersects(b.geom, cc.geom)
where cc.namelsad is null

create table indigo.crop_three_year_average_2018_2020_4326_final as
select c.fips, c.county_or_equivalent , sf."name" as name , c.corn_bushels , c.soybean_bushels , 
c.corn_bushels + c.soybean_bushels as crop_total_avg_2018_2020, c.geom 
from indigo.crop_three_year_average_2018_2020_4326 c
inner join indigo.state_fips sf on substring(c.fips, 1,2) = sf.fips

create index buyers_final_county_stats_sidx
on indigo.buyers_final_county_stats using GIST(geom)

create table indigo.crop_three_year_average_2018_2020_4326_final as
select cc.statefp || cc.countyfp as fips, cc.namelsad as county_or_eqivalent, cc."name" as name, 
cf.crop_total_bushels , cf.corn_bushels , cf.soybean_bushels , cc.geom 
from indigo.county_conus cc 
left join indigo.crop_2020_final cf on cc.statefp || cc.countyfp = cf.fips 
order by fips

create table indigo.crop_2020_all_conus as
select cc.statefp || cc.countyfp as fips, cc.namelsad as county_or_eqivalent, cc."name" as name, 
cf.crop_total_bushels , cf.corn_bushels , cf.soybean_bushels , cc.geom 
from indigo.county_conus cc 
left join indigo.crop_2020_final cf on cc.statefp || cc.countyfp = cf.fips 
order by fips

create table indigo.buyers_final as
select b.id, cc.statefp || cc.countyfp as fips, cc.namelsad as county_or_eqivalent, sf."name" as state,
b.geom 
from indigo.county_conus cc 
left join indigo.buyers_final_work b on b.statefp || b.countyfp = cc.statefp || cc.countyfp 
inner join indigo.state_fips sf on cc.statefp = sf.fips 
where b.geom is not null
order by sf."name" 

update indigo.buyers_final_county_stats 
set crop_total_bushels = 0 where crop_total_bushels is null

create table indigo.buyers_final_county_stats as 
select b.* , cf.crop_total_bushels 
from indigo.buyers_final b
inner join indigo.crop_2020_all_conus cf on b.fips = cf.fips 

create table indigo.crop_three_year_average_2018_2020_4326_included_counties_2 as
select a.* , corn_bushels + soybean_bushels as crop_total_bushels
from indigo.crop_three_year_average_2018_2020_4326_included_counties a

select * from indigo.nass_soybean_production_2018_2020 ncp where county like '%GOLLAD%'