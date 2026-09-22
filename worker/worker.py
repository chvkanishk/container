"""Dice job hunter worker.

Runs on both laptops. The only difference is NODE_NAME in .env.
Loop: wait for the baton -> search Dice -> save new jobs -> pass the baton.
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

NODE = os.getenv("NODE_NAME", "A").strip().upper()
PARTNER = "B" if NODE == "A" else "A"
DB_URL = os.environ["DATABASE_URL"]
CYCLE_MIN = int(os.getenv("CYCLE_MINUTES", "15"))
MAX_AGE_MIN = int(os.getenv("MAX_AGE_MINUTES", "60"))
SEARCHES_FILE = os.getenv("SEARCHES_FILE", "/config/searches.json")
DICE_URL = os.getenv("DICE_MCP_URL", "https://mcp.dice.com/mcp")
JOBS_PER_PAGE = 50
MAX_PAGES = 4                      # safety cap per keyword
STALE_SECS = CYCLE_MIN * 60 * 3    # partner is presumed down after this
POLL_SECS = 15                     # how often we check the baton

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{NODE}] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger().info


def db():
    """A fresh short-lived connection. We never hold one open between runs."""
    return psycopg.connect(DB_URL, autocommit=True, connect_timeout=30)


def clean(value):
    """Empty strings become NULL so 'missing' has only one form."""
    if value is None:
        return None
    value = str(value).strip()
    return value or None


# --------------------------------------------------------------------------
# Baton: whose turn is it?
# --------------------------------------------------------------------------

def try_claim_baton():
    """Atomically take the baton if it is our turn, or if the partner is stale.

    All time comparisons use the database clock, so the two laptops never
    disagree about what time it is.
    """
    sql = """
        UPDATE baton SET current_node = %(me)s, next_start = now()
        WHERE id = 1
          AND (
                (current_node = %(me)s AND now() >= next_start)
             OR  now() > next_start + make_interval(secs => %(stale)s)
          )
        RETURNING last_node
    """
    with db() as conn:
        row = conn.execute(sql, {"me": NODE, "stale": STALE_SECS}).fetchone()
        if row is None:
            return False
        if row[0] == NODE:
            log(f"{PARTNER} looks down, taking over")
        return True


def pass_baton():
    """Hand over to the partner, scheduled one cycle from now."""
    sql = """
        UPDATE baton
           SET current_node = %(partner)s,
               next_start = now() + make_interval(mins => %(cycle)s),
               last_node = %(me)s,
               last_finished = now()
         WHERE id = 1
    """
    with db() as conn:
        conn.execute(sql, {"partner": PARTNER, "me": NODE, "cycle": CYCLE_MIN})
    log(f"Baton passed to {PARTNER}, next run in {CYCLE_MIN} min")


# --------------------------------------------------------------------------
# Run log
# --------------------------------------------------------------------------

def start_run():
    with db() as conn:
        return conn.execute(
            "INSERT INTO runs (node) VALUES (%s) RETURNING id", (NODE,)
        ).fetchone()[0]


def finish_run(run_id, checked, new_jobs, error=None):
    with db() as conn:
        conn.execute(
            """UPDATE runs SET finished_at = now(), checked = %s,
                              new_jobs = %s, error = %s WHERE id = %s""",
            (checked, new_jobs, (error or None), run_id),
        )


# --------------------------------------------------------------------------
# Dice
# --------------------------------------------------------------------------

async def search_dice(cutoff):
    """Return every posting newer than `cutoff`, across all keywords."""
    with open(SEARCHES_FILE) as f:
        searches = json.load(f)

    found = []
    async with streamablehttp_client(DICE_URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            for search in searches:
                keyword = search["keyword"]
                kept = 0
                for page in range(1, MAX_PAGES + 1):
                    args = {
                        **search,
                        "posted_date": "ONE",       # today (Dice's finest filter)
                        "sort": "datePosted",       # newest first
                        "jobs_per_page": JOBS_PER_PAGE,
                        "page_number": page,
                    }
                    result = await session.call_tool("search_jobs", args)
                    if result.isError:
                        raise RuntimeError(f"Dice error for '{keyword}': {result.content}")

                    payload = result.structuredContent or json.loads(result.content[0].text)
                    payload = payload.get("result", payload)
                    jobs = payload.get("data", [])
                    if not jobs:
                        break

                    too_old = False
                    for job in jobs:
                        posted = job.get("postedDate")
                        if posted and datetime.fromisoformat(
                            posted.replace("Z", "+00:00")
                        ) < cutoff:
                            too_old = True     # sorted newest first, so stop here
                            break
                        found.append((keyword, job))
                        kept += 1

                    if too_old or page >= payload.get("metadata", {}).get("totalPages", 1):
                        break
                    await asyncio.sleep(1)      # be polite to Dice

                log(f"  '{keyword}': {kept} recent")
    return found


# --------------------------------------------------------------------------
# Saving
# --------------------------------------------------------------------------

INSERT = """
    INSERT INTO jobs (job_id, title, company, location, workplace,
                      employment_type, salary, url, posted_at, summary,
                      keyword, found_by)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_id) DO NOTHING
    RETURNING job_id
"""


def save(postings):
    """Insert postings, skipping any job_id already in the table."""
    new_count = 0
    with db() as conn:
        for keyword, job in postings:
            row = conn.execute(INSERT, (
                job["guid"],
                clean(job.get("title")),
                clean(job.get("companyName")),
                clean((job.get("jobLocation") or {}).get("displayName")),
                clean(", ".join(job.get("workplaceTypes") or [])),
                clean(job.get("employmentType")),
                clean(job.get("salary")),
                job.get("detailsPageUrl"),
                job.get("postedDate"),
                clean(job.get("summary")),
                keyword,
                NODE,
            )).fetchone()
            if row:
                new_count += 1
    return new_count


# --------------------------------------------------------------------------
# One cycle
# --------------------------------------------------------------------------

async def run_cycle():
    run_id = start_run()
    checked = new_jobs = 0
    error = None
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=MAX_AGE_MIN)
        log(f"Searching Dice for postings since {cutoff:%H:%M} UTC")
        postings = await search_dice(cutoff)
        checked = len(postings)
        new_jobs = save(postings)
        log(f"Checked {checked}, saved {new_jobs} new")
    except Exception as exc:                      # never let one bad run kill the loop
        error = f"{type(exc).__name__}: {exc}"[:1000]
        log(f"Run failed: {error}")
    finally:
        try:
            finish_run(run_id, checked, new_jobs, error)
        except Exception as exc:
            log(f"Could not write run log: {exc}")


async def main():
    log(f"Worker starting. Cycle {CYCLE_MIN} min, max age {MAX_AGE_MIN} min")

    if "--once" in sys.argv:                      # manual test, ignores the baton
        await run_cycle()
        return

    while True:
        try:
            if try_claim_baton():
                await run_cycle()
                pass_baton()
        except Exception as exc:                  # database unreachable, etc.
            log(f"Cycle error: {type(exc).__name__}: {exc}")
        await asyncio.sleep(POLL_SECS)


if __name__ == "__main__":
    asyncio.run(main())
