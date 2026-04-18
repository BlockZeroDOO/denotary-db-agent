if db_id(N'ledger') is null
begin
    create database ledger;
end
go

use ledger;
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
