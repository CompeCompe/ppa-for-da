CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE dds.states DROP CONSTRAINT state_name_uindex CASCADE;
ALTER TABLE dds.states ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.states ADD column created_date timestamp default now();

ALTER TABLE dds.directions DROP CONSTRAINT direction_code_uindex cascade;
ALTER TABLE dds.directions ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.directions ADD column created_date timestamp default now();

ALTER TABLE dds.levels DROP CONSTRAINT level_name_uindex CASCADE;
ALTER TABLE dds.levels ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.levels ADD column created_date timestamp default now();

ALTER TABLE dds.editors DROP CONSTRAINT editors_uindex CASCADE ;
ALTER TABLE dds.editors ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.editors ADD column created_date timestamp default now();

ALTER TABLE dds.units DROP CONSTRAINT units_uindex CASCADE ;
ALTER TABLE dds.units ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.units ADD column created_date timestamp default now();

ALTER TABLE dds.up ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.up ADD column created_date timestamp default now();

ALTER TABLE dds.wp ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.wp ADD column created_date timestamp default now();

ALTER TABLE dds.wp_editor ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.wp_editor ADD column created_date timestamp default now();

ALTER TABLE dds.wp_up ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.wp_up ADD column created_date timestamp default now();

ALTER TABLE dds.wp_markup DROP CONSTRAINT wp_id_uindex CASCADE ;
ALTER TABLE dds.wp_markup ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.wp_markup ADD column created_date timestamp default now();

ALTER TABLE dds.online_courses ADD column guid uuid default uuid_generate_v4();
ALTER TABLE dds.online_courses ADD column created_date timestamp default now();