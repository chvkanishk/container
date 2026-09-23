# Dice job hunter

Two laptops (A and B) take turns searching Dice and save new postings into a
shared Supabase Postgres database. No duplicates: Dice's job id is the primary key.

## One-time database setup

Open Supabase > SQL Editor and run `schema.sql`. It creates the `baton`,
`runs` and `jobs` tables and the single baton row. It is safe to re-run.

## Setup on each laptop

1. git clone this repo
2. cp .env.example .env  and fill it in
   - DATABASE_URL: Supabase > Project Settings > Database > Session pooler
   - NODE_NAME: A on one laptop, B on the other
3. docker compose up -d --build

## Useful commands

    docker compose logs -f                       # watch it work
    docker compose run --rm worker python worker.py --once   # one manual run
    docker compose restart                       # after a git pull

## Settings (.env)

    CYCLE_MINUTES    minutes between handoffs (5 for testing, 15 normal)
    MAX_AGE_MINUTES  ignore postings older than this (keep well above 4 x CYCLE_MINUTES)

Keywords live in config/searches.json and must be the same on both laptops.

## How the handoff works

One row in the `baton` table says whose turn it is and when they may start.
A worker claims the baton only if it is its turn, searches, then sets the
baton to its partner with a start time one cycle later. If the partner has
not taken its turn for 3 cycles, the other laptop takes over, so one machine
being off never stops the loop. All times come from the database clock.
