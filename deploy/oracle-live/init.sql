begin
    execute immediate 'drop table payments purge';
exception
    when others then
        if sqlcode != -942 then
            raise;
        end if;
end;
/

begin
    execute immediate 'drop table invoices purge';
exception
    when others then
        if sqlcode != -942 then
            raise;
        end if;
end;
/

begin
    execute immediate '
        create table invoices (
            id number(19) not null primary key,
            status varchar2(32 char) not null,
            amount number(18,2) not null,
            updated_at timestamp not null
        )';
end;
/

begin
    execute immediate '
        create table payments (
            id number(19) not null primary key,
            invoice_id number(19) not null,
            amount number(18,2) not null,
            updated_at timestamp not null
        )';
end;
/
