-- Run once in Supabase > SQL Editor. Safe to re-run: nothing is dropped.

create table if not exists jobs (
  job_id          text primary key,      -- Dice's unique ID: blocks duplicates
  title           text not null,
  company         text,
  location        text,
  workplace       text,                  -- Remote / Hybrid / On-Site
  employment_type text,
  salary          text,
  url             text not null,         -- the Dice link
  posted_at       timestamptz,           -- when Dice published it
  summary         text,
  keyword         text,                  -- which of your searches found it
  found_by        text,                  -- 'A' or 'B'
  found_at        timestamptz not null default now()
);

create index if not exists jobs_found_at_idx on jobs (found_at desc);

-- The baton: a single row A and B hand back and forth
create table if not exists baton (
  id            int primary key default 1,
  current_node  text not null,           -- whose turn it is
  next_start    timestamptz not null,    -- when they may start
  last_node     text,
  last_finished timestamptz,
  constraint one_row check (id = 1)
);

insert into baton (current_node, next_start) values ('A', now())
on conflict (id) do nothing;

-- A log of every run, so you can tell quiet days from broken containers
create table if not exists runs (
  id           bigserial primary key,
  node         text not null,
  started_at   timestamptz not null default now(),
  finished_at  timestamptz,
  checked      int default 0,
  new_jobs     int default 0,
  error        text
);
