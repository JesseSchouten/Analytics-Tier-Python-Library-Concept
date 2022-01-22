# Dutchchannels main python library

## Introduction

This library currently contains modules grouped around data sources and destinations:

- **dc_azure_toolkit**: first setup of connectors to azure services
- **data_sql**: authentication and CRUD of tables inside the sql-server project
- **data_blob**: authentication and read files from Azure blob storage
- **data_cosmos**: authentication and read files from Azure Cosmos database
- **data_generic**: generic functions that can be used in all sources

## Dutchchannels Azure Toolkit

dc_azure_toolkit currently supports the following resources in some shape or form:

- Azure Data Explorer (adx.py)
- Azure Cosmos Database (cosmos_db.py)
- SQL Server (sql.py)

## Data SQL

This module supports integration with tables in the sql-server project.

- See `/analytics-tier/sql-server/dc/dbo/Tables` for an overview of all table definitions
- this module follows the [repository](https://www.cosmicpython.com/book/chapter_02_repository.html), [service layert](https://www.cosmicpython.com/book/chapter_04_service_layer.html) and [DOA](https://en.wikipedia.org/wiki/Data_access_object) patterns
