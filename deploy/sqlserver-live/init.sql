if db_id(N'ledger') is null
begin
    create database ledger;
end
go

use ledger;
go

if not exists (
    select 1 from sys.change_tracking_databases where database_id = db_id()
)
begin
    alter database ledger
    set change_tracking = on
    (change_retention = 2 days, auto_cleanup = on);
end
go

if object_id(N'dbo.invoices', N'U') is null
begin
    create table dbo.invoices (
        id bigint not null primary key,
        status nvarchar(32) not null,
        amount decimal(18, 2) not null,
        updated_at datetime2 not null
    );
end
go

if not exists (
    select 1
    from sys.change_tracking_tables ctt
    join sys.tables t on ctt.object_id = t.object_id
    join sys.schemas s on t.schema_id = s.schema_id
    where s.name = N'dbo' and t.name = N'invoices'
)
begin
    alter table dbo.invoices
    enable change_tracking
    with (track_columns_updated = on);
end
go

if object_id(N'dbo.payments', N'U') is null
begin
    create table dbo.payments (
        id bigint not null primary key,
        invoice_id bigint not null,
        amount decimal(18, 2) not null,
        updated_at datetime2 not null
    );
end
go

if not exists (
    select 1
    from sys.change_tracking_tables ctt
    join sys.tables t on ctt.object_id = t.object_id
    join sys.schemas s on t.schema_id = s.schema_id
    where s.name = N'dbo' and t.name = N'payments'
)
begin
    alter table dbo.payments
    enable change_tracking
    with (track_columns_updated = on);
end
go
