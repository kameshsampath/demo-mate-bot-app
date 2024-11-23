create database if not exists <%ctx.env.DEMO_DB%>;
use database <%ctx.env.DEMO_DB%>;

create schema if not exists  <%ctx.env.DEMO_SCHEMA%>;

use schema <%ctx.env.DEMO_SCHEMA%>;

create or replace file format csvformat
  skip_header = 1
  field_optionally_enclosed_by = '"'
  type = 'CSV';

create or replace stage support_tickets_data_stage
  file_format = csvformat
  url = 's3://sfquickstarts/finetuning_llm_using_snowflake_cortex_ai/';

create or replace table SUPPORT_TICKETS (
  ticket_id VARCHAR(60),
  customer_name VARCHAR(60),
  customer_email VARCHAR(60),
  service_type VARCHAR(60),
  request VARCHAR,
  contact_preference VARCHAR(60)
);

copy into SUPPORT_TICKETS
  from @support_tickets_data_stage;