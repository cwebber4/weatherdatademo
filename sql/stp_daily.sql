-- Copyright 2023 Chris Webber

create or replace function ghcn.stp_daily()
returns void

AS $$

BEGIN
create temporary table daily_tmp
(
    ID bigserial NOT NULL,
    StationID varchar(11) NOT NULL,
    Date date not null,
    Element varchar(4) not null,
    Value NUMERIC(5),
    MFlag varchar(1),
    QFlag varchar(1),
    SFlag varchar(1),
    OBSTime time without time zone
);

insert into daily_tmp
(
    StationID,
    Date,
    Element,
    Value,
    MFlag,
    QFlag,
    SFlag,
    OBSTime
)
select
    StationID,
    Date,
    Element,
    Value,
    MFlag,
    QFlag,
    SFlag,
    OBSTime
from ghcn.daily_raw r
where r.ID in (
    select max(ID)
    from ghcn.daily_raw
    group by StationID, Date, OBSTime, Element
);

INSERT INTO ghcn.daily
(
    StationID
    ,Date
    ,OBSTime
)
select 
    distinct
    t.StationID
    ,t.Date
    ,t.OBSTime
from daily_tmp t
left join ghcn.daily d on
    d.StationID = t.StationID
    and d.Date = t.Date
    and (d.OBSTime = t.OBSTime OR (d.OBSTime IS NULL AND t.OBSTime IS NULL))
where d.ID is NULL;

UPDATE ghcn.daily as d
    set TMAX = t.Value
from daily_tmp t
where
    d.StationID = t.StationID
    and d.Date = t.Date
    and (d.OBSTime = t.OBSTime OR (d.OBSTime IS NULL AND t.OBSTime IS NULL))
    and t.element = 'TMAX';

UPDATE ghcn.daily as d
    set TMIN = t.Value
from daily_tmp t
where
    d.StationID = t.StationID
    and d.Date = t.Date
    and (d.OBSTime = t.OBSTime OR (d.OBSTime IS NULL AND t.OBSTime IS NULL))
    and t.element = 'TMIN';

UPDATE ghcn.daily as d
    set PRCP = t.Value
from daily_tmp t
where
    d.StationID = t.StationID
    and d.Date = t.Date
    and (d.OBSTime = t.OBSTime OR (d.OBSTime IS NULL AND t.OBSTime IS NULL))
    and t.element = 'PRCP';

UPDATE ghcn.daily as d
    set SNOW = t.Value
from daily_tmp t
where
    d.StationID = t.StationID
    and d.Date = t.Date
    and (d.OBSTime = t.OBSTime OR (d.OBSTime IS NULL AND t.OBSTime IS NULL))
    and t.element = 'SNOW';

drop table daily_tmp;
delete from ghcn.daily_raw;

END $$ LANGUAGE plpgsql