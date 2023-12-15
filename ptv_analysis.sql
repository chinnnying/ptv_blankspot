-----------------------------------------------------------------------
-------------------- task a                          ------------------
-----------------------------------------------------------------------


--1.1 

CREATE SCHEMA ptv;


--1.2

/*drop table ptv.agency;
drop table ptv.calendar_dates;
drop table ptv.calendar;
drop table ptv.routes;
drop table ptv.shapes;
drop table ptv.stops;
drop table ptv.stop_times;
drop table ptv.trips;
drop table ptv.lga2021;
drop table ptv.suburb2021;
drop table ptv.mb2021;*/

------------ agency ---------------
--drop table ptv.agency;
create table ptv.agency(
agency_id numeric,
agency_name varchar,
agency_url varchar,
agency_timezone varchar,
agency_lang varchar
);

COPY ptv.agency(agency_id,agency_name,agency_url,agency_timezone,agency_lang)
FROM '/data/adata/gtfs/agency.txt'
delimiter ','
csv header;

select * from ptv.agency;

------------ calendar_dates ---------------
--drop table ptv.calendar_dates;
create table ptv.calendar_dates(
service_id varchar,
"date" date, -- come back and check
exception_type varchar -- can be varchar
);

COPY ptv.calendar_dates(service_id,"date",exception_type)
FROM '/data/adata/gtfs/calendar_dates.txt'
delimiter ','
csv header;

select * from ptv.calendar_dates;

------------ calendar ---------------
--drop table ptv.calendar;
create table ptv.calendar(
service_id varchar,
monday boolean,
tuesday boolean,
wednesday boolean, 
thursday boolean,
friday boolean,
saturday boolean,
sunday boolean,
start_date date,
end_date date
);

COPY ptv.calendar(service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date)
FROM '/data/adata/gtfs/calendar.txt'
delimiter ','
csv header;

select * from ptv.calendar;

------------ routes ---------------
create table ptv.routes(
route_id varchar,
agency_id numeric,
route_short_name varchar,
route_long_name varchar,
route_type numeric, -- ?
route_color varchar,
route_text_color varchar
);

COPY ptv.routes(route_id,agency_id,route_short_name,route_long_name,route_type,route_color,route_text_color)
FROM '/data/adata/gtfs/routes.txt'
delimiter ','
csv header;

select * from ptv.routes;

------------ shapes ---------------
create table ptv.shapes(
shape_id varchar,
shape_pt_lat float,
shape_pt_lon float,
shape_pt_sequence numeric,
shape_dist_traveled float
);

COPY ptv.shapes(shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled)
FROM '/data/adata/gtfs/shapes.txt'
delimiter ','
csv header;

select * from ptv.shapes;

------------ stop times ---------------
--drop table ptv.stop_times;
create table ptv.stop_times(
trip_id varchar,
arrival_time varchar, -- date/time field value out of range: "24:19:00"
departure_time varchar, -- date/time field value out of range: "24:19:00"
stop_id numeric,
stop_sequence numeric,
stop_headsign varchar,
pickup_type boolean, 
drop_off_type boolean,
shape_dist_traveled varchar -- invalid input syntax for type double precision: ""
);

COPY ptv.stop_times(trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled)
FROM '/data/adata/gtfs/stop_times.txt'
delimiter ','
csv header;

UPDATE ptv.stop_times 
SET arrival_time = 
    CASE 
        WHEN CAST(LEFT(arrival_time, 2) AS INTEGER) >= 24 THEN
            TO_CHAR(
                CAST(
                    CONCAT(
                        CAST(CAST(LEFT(arrival_time, 2) AS INTEGER) - 24 AS TEXT),
                        SUBSTRING(arrival_time FROM 3)
                    ) AS TIME
                ),
                'HH24:MI:SS'
            )
        ELSE 
            arrival_time
    END
WHERE CAST(LEFT(arrival_time, 2) AS INTEGER) >= 24;

UPDATE ptv.stop_times 
SET departure_time = 
    CASE 
        WHEN CAST(LEFT(departure_time, 2) AS INTEGER) >= 24 THEN
            TO_CHAR(
                CAST(
                    CONCAT(
                        CAST(CAST(LEFT(departure_time, 2) AS INTEGER) - 24 AS TEXT),
                        SUBSTRING(departure_time FROM 3)
                    ) AS TIME
                ),
                'HH24:MI:SS'
            )
        ELSE 
            departure_time
    END
WHERE CAST(LEFT(departure_time, 2) AS INTEGER) >= 24;

  
ALTER TABLE ptv.stop_times ALTER COLUMN arrival_time TYPE time USING arrival_time::time;
ALTER TABLE ptv.stop_times ALTER COLUMN departure_time TYPE time USING departure_time::time;



ALTER TABLE ptv.stop_times 
ALTER COLUMN shape_dist_traveled TYPE float8 
USING CASE 
    WHEN shape_dist_traveled ~ '^[+-]?[0-9]+([.][0-9]*)?$' THEN shape_dist_traveled::float8
    ELSE NULL 
END;

    

select pg_typeof(arrival_time), pg_typeof(departure_time),  pg_typeof(shape_dist_traveled) from ptv.stop_times; -- check data type

select * from ptv.stop_times;

---------------- stops ------------------

create table ptv.stops(
stop_id numeric,
stop_name varchar,
stop_lat float,
stop_lon float
);

COPY ptv.stops(stop_id,stop_name,stop_lat,stop_lon)
FROM '/data/adata/gtfs/stops.txt'
delimiter ','
csv header;

select * from ptv.stops;


---------------- trips ------------------

create table ptv.trips(
route_id varchar,
service_id varchar,
trip_id varchar,
shape_id varchar,
trip_headsign varchar,
direction_id boolean
);

COPY ptv.trips(route_id,service_id,trip_id,shape_id,trip_headsign,direction_id)
FROM '/data/adata/gtfs/trips.txt'
delimiter ','
csv header;

select * from ptv.trips;


--- check number of rows
select count(*) from ptv.agency; -- 10
select count(*) from ptv.calendar; -- 380
select count(*) from ptv.calendar_dates; -- 15
select count(*) from ptv.routes; -- 3300
select count(*) from ptv.shapes; -- 9757418
select count(*) from ptv.stop_times; -- 8122810
select count(*) from ptv.stops; -- 27821
select count(*) from ptv.trips; -- 236613


--1.4
------------ suburb2021 ---------------
--drop table ptv.suburb2021;
create table ptv.suburb2021(
MB_CODE_2021 varchar,
SAL_CODE_2021 varchar,
SAL_NAME_2021 varchar,
STATE_CODE_2021 varchar,
STATE_NAME_2021 varchar,
AUS_CODE_2021 varchar,
AUS_NAME_2021 varchar,
AREA_ALBERS_SQKM float,
ASGS_LOCI_URI_2021 varchar
);

COPY ptv.suburb2021(MB_CODE_2021,SAL_CODE_2021,SAL_NAME_2021,STATE_CODE_2021,STATE_NAME_2021,AUS_CODE_2021,AUS_NAME_2021,AREA_ALBERS_SQKM,ASGS_LOCI_URI_2021)
FROM '/data/adata/SAL_2021_AUST.csv'
delimiter ','
csv header;

select * from ptv.suburb2021;

------------ lga2021 ---------------
-- drop table ptv.lga2021;
create table ptv.lga2021(
MB_CODE_2021 varchar,
LGA_CODE_2021 varchar,
LGA_NAME_2021 varchar,
STATE_CODE_2021 varchar,
STATE_NAME_2021 varchar,
AUS_CODE_2021 varchar,
AUS_NAME_2021 varchar,
AREA_ALBERS_SQKM float,
ASGS_LOCI_URI_2021 varchar
);

COPY ptv.lga2021(MB_CODE_2021,LGA_CODE_2021,LGA_NAME_2021,STATE_CODE_2021,STATE_NAME_2021,AUS_CODE_2021,AUS_NAME_2021,AREA_ALBERS_SQKM,ASGS_LOCI_URI_2021)
FROM '/data/adata/LGA_2021_AUST.csv'
delimiter ','
csv header;

select * from ptv.lga2021;

--- check number of rows
--select count(*) from ptv.suburb2021; -- 368286
--select count(*) from ptv.lga2021; -- 368286
--select count(*) from ptv.mb2021; -- 368286
--SELECT * FROM ptv.mb2021;


--1.5
with tbl as
(select table_schema, TABLE_NAME
 from information_schema.tables
 where table_schema in ('ptv'))
select table_schema, TABLE_NAME,
(xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I.%I', table_schema, TABLE_NAME), FALSE, TRUE, '')))[1]::text::int AS rows_n
from tbl
order by table_name; 

-----------------------------------------------------------------------
-------------------- task b                          ------------------
-----------------------------------------------------------------------

select count(*) from ptv.mb2021;

-- 2.1
create table ptv.mb2021_mel as
select * from ptv.mb2021
where lower(gcc_name21) = 'greater melbourne';

--select * from ptv.mb2021_mel;
--select count(*) from ptv.mb2021_mel;

-- 2.2
--drop table ptv.melbourne;
create table ptv.melbourne as
SELECT gcc_name21, ST_Union(wkb_geometry) AS mel_geometry
FROM ptv.mb2021_mel
group by gcc_name21;

select * from ptv.melbourne;

-- 2.3
select  AddGeometryColumn ('ptv','stops','geom',7844,'POINT',2);

update ptv.stops 
set geom = ST_SetSRID(ST_Point(stop_lon, stop_lat), 7844);

select * from ptv.stops;

-- to check stop with same id but different geom
/*SELECT 
    a.stop_id, 
    a.stop_lat AS stop_lat_a, 
    a.stop_lon AS stop_lon_a, 
    b.stop_lat AS stop_lat_b, 
    b.stop_lon AS stop_lon_b 
FROM ptv.stops AS a 
JOIN ptv.stops AS b 
    ON a.stop_id = b.stop_id 
    AND (a.stop_lat <> b.stop_lat and a.stop_lon <> b.stop_lon);*/


-- 2.4
CREATE INDEX idx_stop ON ptv.stops USING GIST(geom);
CREATE INDEX idx_mel ON ptv.melbourne USING GIST(mel_geometry);

--drop table ptv.stops_routes_mel;
create table ptv.stops_routes_mel as
select distinct s.stop_id, s.stop_name, s.geom, r.route_short_name route_number, r.route_long_name route_name,
	case 
        when r.route_type = 0 then 'Tram'
        when r.route_type = 2 then 'Train'
        when r.route_type = 3 then 'Bus'
        else 'Unknown'
    end vehicle
from ptv.routes r
join ptv.trips t
on (r.route_id = t.route_id)
join ptv.stop_times st
on (t.trip_id = st.trip_id)
join ptv.stops s
on (st.stop_id = s.stop_id)
join ptv.melbourne m 
on ST_Contains(m.mel_geometry, s.geom);


select * from ptv.stops_routes_mel;

-- 2.4.1
select count(*) from ptv.stops_routes_mel; --> 31614

-- 2.4.2
select count(distinct stop_id) count_of_unique_stop_id from ptv.stops_routes_mel; --> 20644

select count(*) as count_of_distinct_stop from (select distinct stop_id, geom from ptv.stops_routes_mel) as count; -->20772


-----------------------------------------------------------------------
-------------------- task c                          ------------------
-----------------------------------------------------------------------

-- 3.1
--select * from ptv.lga2021;
--select * from ptv.suburb2021;
--select * from ptv.stops_routes_mel;
--select * from ptv.mb2021_mel;


----------- before --------------------
select
s.sal_name_2021 as suburb,
count(sr.stop_id) as suburb_stops
from
ptv.suburb2021 s
left join ptv.mb2021_mel m on s.mb_code_2021 = m.mb_code21
left join ptv.stops_routes_mel sr on st_contains(m.wkb_geometry, sr.geom) and sr.vehicle = 'Bus'
where lower(gcc_name21) = 'greater melbourne'
group by
s.sal_name_2021
order by
suburb_stops, s.sal_name_2021;

rollback;

----------- improved --------------------

CREATE INDEX idx_geom ON ptv.stops_routes_mel USING GIST(geom);

with temp_table as (
    select 
        sb.sal_name_2021,
        s.stop_id,
        s.geom
    from 
        ptv.suburb2021 sb
    left join 
        ptv.mb2021_mel mb
    on 
        mb.mb_code21 = sb.mb_code_2021
    left join 
        ptv.stops_routes_mel s
    on  
        st_contains(mb.wkb_geometry, s.geom)
    join 
        ptv.mb2021_mel mb2
    on 
        mb2.mb_code21 = sb.mb_code_2021
    group by 
        sb.sal_name_2021, s.stop_id, s.geom
)

select 
    sal_name_2021 suburb,
    count(stop_id) as no_distinct_stop
from 
    temp_table
group by 
    suburb
order by 
    no_distinct_stop, suburb;


-- 3.1.1
   
with temp_table as (
    select 
        sb.sal_name_2021,
        s.stop_id,
        s.geom
    from 
        ptv.suburb2021 sb
    left join 
        ptv.mb2021_mel mb
    on 
        mb.mb_code21 = sb.mb_code_2021
    left join 
        ptv.stops_routes_mel s
    on  
        st_contains(mb.wkb_geometry, s.geom)
    join 
        ptv.mb2021_mel mb2
    on 
        mb2.mb_code21 = sb.mb_code_2021
    group by 
        sb.sal_name_2021, s.stop_id, s.geom
)

select 
    sal_name_2021 suburb,
    count(stop_id) as no_distinct_stop
from 
    temp_table
group by 
    suburb
order by 
    no_distinct_stop, suburb
limit 5;

-- 3.1.2

with temp_table as (
select
sb.sal_name_2021,
s.stop_id,
s.geom
from
ptv.suburb2021 sb
left join
ptv.mb2021_mel mb
on
mb.mb_code21 = sb.mb_code_2021
left join
ptv.stops_routes_mel s
on
st_contains(mb.wkb_geometry, s.geom)
join
ptv.mb2021_mel mb2
on
mb2.mb_code21 = sb.mb_code_2021
group by
sb.sal_name_2021, s.stop_id, s.geom
), suburb_stops as (
select
sal_name_2021 suburb,
count(distinct stop_id) as no_distinct_stop
from
temp_table
group by
suburb)
   
select sum(no_distinct_stop) as average_stops

from 
    suburb_stops;
   

-- 3.2

CREATE INDEX idx_mb2021_mel_geom ON ptv.mb2021_mel USING GIST(wkb_geometry);
CREATE INDEX idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);

--drop table ptv.residential_bs; 
CREATE TABLE ptv.residential_bs AS
WITH temp_not_blank AS (
    SELECT 
        DISTINCT mb.mb_code21
    FROM 
        ptv.mb2021_mel mb
    JOIN 
        ptv.stops_routes_mel s
    ON 
        ST_Contains(mb.wkb_geometry, s.geom)
    WHERE
        s.vehicle = 'Bus' AND mb.mb_cat21 = 'Residential'
)

select 
    l.lga_name_2021,
    count(*) total_mb,
    SUM(case when mb.mb_code21 not in (select mb_code21 from temp_not_blank) THEN 1 END) total_blank_spot,
    SUM(case when mb.mb_code21 not in (select mb_code21 from temp_not_blank) THEN 1 END):: float * 100 / COUNT(*) percentage
from
    ptv.mb2021_mel mb
left join 
    ptv.lga2021 l
on
    (mb.mb_code21 = l.mb_code_2021)
where
    mb.mb_cat21 = 'Residential'
group by
    l.lga_name_2021
order by
    total_blank_spot asc; 

-- 3.2.1

select avg(percentage) from ptv.residential_bs;    
 
select * from ptv.residential_bs order by percentage;   

-----------------------------------------------------------------------
-------------------- task d                          ------------------
-----------------------------------------------------------------------

-- 4.1
create table ptv.lga_blankspot as
with temp_blank as (
select distinct mb.mb_code21
from ptv.mb2021_mel mb
where mb.mb_cat21 = 'Residential'
and not exists (
select 1
from ptv.stops_routes_mel s
where s.vehicle = 'Bus'
and st_contains(mb.wkb_geometry, s.geom) )
)
select
l.lga_name_2021 as lga,
sum(case when mb.mb_code21 in (select mb_code21 from temp_blank) then 1 else 0 end)::float * 100 / count(*) as percentage,
st_union(mb.wkb_geometry) as mel_geometry
from ptv.mb2021_mel mb
join ptv.lga2021 l on mb.mb_code21 = l.mb_code_2021
where mb.mb_cat21 = 'Residential'
group by l.lga_name_2021
order by percentage;

--Code used for visualisation to visualise non-residential areas in QGIS as well.
CREATE TABLE ptv.residential_and_other AS
WITH temp_not_blank AS (
SELECT DISTINCT mb.mb_code21
FROM ptv.mb2021_mel mb
JOIN ptv.stops_routes_mel s ON ST_Contains(mb.wkb_geometry, s.geom)
WHERE s.vehicle = 'Bus' AND mb.mb_cat21 = 'Residential'
)
SELECT
l.lga_name_2021,
COUNT(*) AS total_mb,
SUM(CASE WHEN mb.mb_code21 NOT IN (SELECT mb_code21 FROM temp_not_blank) THEN 1 ELSE 0 END) AS total_blank_spot,
CASE WHEN mb.mb_cat21 = 'Residential'
THEN
SUM(CASE WHEN mb.mb_code21 NOT IN (SELECT mb_code21 FROM temp_not_blank) THEN 1 ELSE 0 END) * 100 / COUNT(*)
ELSE
NULL
END AS percentage,
St_Union(mb.wkb_geometry) AS mel_geom
FROM ptv.mb2021_mel mb
JOIN ptv.lga2021 l ON (mb.mb_code21 = l.mb_code_2021)
GROUP BY l.lga_name_2021, mb.mb_cat21
ORDER BY percentage;


UPDATE ptv.residential_and_other SET percentage=0 WHERE percentage IS null;




