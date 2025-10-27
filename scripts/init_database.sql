/*

Create Database and Schemas 

Script Purpose:
    This script creates a new database named 'RugbyDataWarehouse' after checking the if it already exsits.
    If the database exsists, it is dropped and recreated. Additionally, he script sets up 3 schemas within the database:
    'bronze', 'silver', 'gold'.

WARNING:
    Running the script will drop the entire 'DataWareHouse' database if it exsists.
    All data in the database will be perminatnly deleted. Proceed with caution and esure you have proper backups before running
    this script. 

*/


-- Drop and recreate 'DataWhareHouse' database

do 
$$
begin
    if exists (select 1 from pg_database where datname = 'RugbyDataWarehouse') then 
        perform pg_terminate_backend(pid)
        from pg_stat_activity
        where datname = 'RugbyDataWarehouse';

        execute datname =  'drop DATABASE RugbyDataWarehouse';
    end if; 
end
$$;

\gexec
-- create the 'DataWarehouse' database
create DATABASE RugbyDataWarehouse;
\connect RugbyDataWarehouse



-- Connect to the new database manually, 

--create schemas
