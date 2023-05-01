-- Copyright 2023 Chris Webber

drop table if exists ghcn.daily_raw;
CREATE TABLE ghcn.daily_raw
(
    ID bigserial NOT NULL,
    StationID varchar(11) NOT NULL,
    Date date not null,
    Element varchar(4) not null,
    Value NUMERIC(5),
    MFlag varchar(1),
    QFlag varchar(1),
    SFlag varchar(1),
    OBSTime time without time zone,
    DateCreated timestamp default NOW(),
    PRIMARY KEY (ID)
);
    
drop table if exists ghcn.element;
CREATE TABLE ghcn.element
(
    ID serial NOT NULL,
    Name varchar(4) not null,
    Description varchar(45),
    PRIMARY KEY (ID)
);

INSERT INTO ghcn.element
("name", description)
values
('PRCP', 'Precipitation (tenths of mm)')
,('SNOW', 'Snowfall (mm)')
,('SNWD', 'Snow depth (mm)')
,('TMAX', 'Maximum temperature (tenths of degrees C)')
,('TMIN', 'Minimum temperature (tenths of degrees C)');

drop table if exists ghcn.mflag;
CREATE TABLE ghcn.mflag
(
    ID serial not null,
    flag varchar(1) not null,
    description varchar(105),
    PRIMARY KEY(id)
);

insert into ghcn.mflag
(flag, description)
values
('B', 'precipitation total formed from two 12-hour totals')
,('D', 'precipitation total formed from four six-hour totals')
,('H', 'represents highest or lowest hourly temperature (TMAX or TMIN) or the average of hourly values (TAVG)')
,('K', 'converted from knots')
,('L', 'temperature appears to be lagged with respect to reported hour of observation')
,('O', 'converted from oktas')
,('P', 'identified as "missing presumed zero" in DSI 3200 and 3206')
,('T', 'trace of precipitation, snowfall, or snow depth')
,('W', 'converted from 16-point WBAN code (for wind direction)');

drop table if exists ghcn.qflag;
create table ghcn.qflag
(
    id serial primary key not null,
    flag varchar(1) not null,
    description varchar(60)
);

insert into ghcn.qflag
(flag, description)
values
('D', 'failed duplicate check')
,('G', 'failed gap check')
,('I', 'failed internal consistency check')
,('K', 'failed streak/frequent-value check')
,('L', 'failed check on length of multiday period')
,('M', 'failed megaconsistency check')
,('N', 'failed naught check')
,('O', 'failed climatological outlier check')
,('R', 'failed lagged range check')
,('S', 'failed spatial consistency check')
,('T', 'failed temporal consistency check')
,('W', 'temperature too warm for snow')
,('X', 'failed bounds check')
,('Z', 'flagged as a result of an official Datzilla investigation');

drop table if exists ghcn.sflag;
create table ghcn.sflag
(
    ID serial primary key not null,
    flag varchar(1) not null,
    description varchar(122)
);

insert into ghcn.sflag
(flag, description)
values
('0', 'U.S. Cooperative Summary of the Day (NCDC DSI-3200)')
,('6', 'CDMP Cooperative Summary of the Day (NCDC DSI-3206)')
,('7', 'U.S. Cooperative Summary of the Day via WxCoder3 (NCDC DSI-3207)')
,('A', 'U.S. Automated Surface Observing System (ASOS) real-time data')
,('a', 'Australian data from the Australian Bureau of Meteorology')
,('B', 'U.S. ASOS data for October 2000-December 2005 (NCDC DSI-3211)')
,('b', 'Belarus update')
,('C', 'Environment Canada')
,('D', 'Short time delay US National Weather Service CF6 daily summaries provided by the High Plains Regional Climate Center')
,('E', 'European Climate Assessment and Dataset (Klein Tank et al., 2002)')
,('F', 'U.S. Fort data')
,('G', 'Official Global Climate Observing System (GCOS) or other government-supplied data')
,('H', 'High Plains Regional Climate Center real-time data')
,('I', 'International collection (non U.S. data received through personal contacts)')
,('K', 'U.S. Cooperative Summary of the Day data digitized from paper observer forms')
,('M', 'Monthly METAR Extract (additional ASOS data)')
,('m', 'Data from the Mexican National Water Commission (Comision National del Agua -- CONAGUA)')
,('N', 'Community Collaborative Rain, Hail,and Snow (CoCoRaHS)')
,('Q', 'Data from several African countries that had been "quarantined" from public release until permission was granted')
,('R', 'NCEI Reference Network Database (Climate Reference Network and Regional Climate Reference Network)')
,('r', 'All-Russian Research Institute of Hydrometeorological Information-World Data Center')
,('S', 'Global Summary of the Day (NCDC DSI-9618)')
,('s', 'China Meteorological Administration/National Meteorological Information Center/Climatic Data Center')
,('T', 'SNOwpack TELemtry (SNOTEL) data obtained from the U.S. Department of Agriculture''s Natural Resources Conservation Service')
,('U', 'Remote Automatic Weather Station (RAWS) data obtained from the Western Regional Climate Center')
,('u', 'Ukraine update')
,('W', 'WBAN/ASOS Summary of the Day from NCDC''s Integrated Surface Data (ISD)')
,('X', 'U.S. First-Order Summary of the Day (NCDC DSI-3210)')
,('Z', 'Datzilla official additions or replacements')
,('z', 'Uzbekistan update');

drop table if exists ghcn.station;
create table ghcn.station
(
    ID varchar(11) not null,
    Latitude DECIMAL(7,4),
    Longitude DECIMAL(7,4),
    Elevation DECIMAL(5,1),
    State varchar(2),
    Name varchar(30),
    GSNFlag varchar(3),
    HCNCRNFlag varchar(3),
    WMOID varchar(5),
    PRIMARY KEY (ID)
);

alter table if exists ghcn.station
    add Enabled boolean default false;

drop table if exists ghcn.daily;
CREATE TABLE ghcn.daily
(
    ID bigserial NOT NULL,
    StationID varchar(11) NOT NULL,
    Date date not null,
    OBSTime time without time zone,
    TMAX NUMERIC(5),
    TMIN NUMERIC(5),
    TAVG NUMERIC(5),
    PRCP NUMERIC(5),
    SNOW NUMERIC(5),
    DateCreated timestamp default NOW(),
    DateModified timestamp,
    PRIMARY KEY (ID),
    FOREIGN KEY (StationID) REFERENCES ghcn.station (ID)
);

alter table if exists ghcn.daily
    drop column TAVG;
    
create or replace function fn_date_modified_stamp() 
returns trigger
as
$$
BEGIN
    NEW.DateModified := current_timestamp;
    RETURN NEW;
END;
$$ language plpgsql;

create or replace trigger tg_date_modified_stamp
before insert or update
on ghcn.daily
for each row execute function fn_date_modified_stamp();

