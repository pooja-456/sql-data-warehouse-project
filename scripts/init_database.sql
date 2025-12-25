/*
======================================================================
Create Database and Schemas
======================================================================

Script Purpose:

    This script creates a new database named 'DataWarehouse'.
    It switches the context to the 'master' database before creation.
    After creating the database, the script switches to 'DataWarehouse'
    and creates three schemas within it: 'bronze', 'silver', and 'gold'.

    These schemas are typically used to organize data into different
    processing stages.

NOTE:

    Ensure the database name does not already exist before running
    this script, or run a drop script first if recreation is required.

*/


-- Create Database 'DataWarehouse'
use master;
go
create database DataWarehouse;

use DataWarehouse;

-- Create schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
