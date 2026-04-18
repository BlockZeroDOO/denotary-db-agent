create table if not exists invoices (
    id bigint primary key,
    status varchar(32) not null,
    amount decimal(18,2) not null,
    updated_at datetime not null
);

create table if not exists payments (
    id bigint primary key,
    invoice_id bigint not null,
    amount decimal(18,2) not null,
    updated_at datetime not null
);

grant replication slave, replication client on *.* to 'denotary'@'%';
flush privileges;
