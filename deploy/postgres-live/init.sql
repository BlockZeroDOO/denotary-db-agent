create table if not exists public.invoices (
    id bigint primary key,
    status text not null,
    amount numeric(18,2) not null,
    updated_at timestamptz not null
);

create table if not exists public.payments (
    id bigint primary key,
    invoice_id bigint not null,
    amount numeric(18,2) not null,
    updated_at timestamptz not null
);

