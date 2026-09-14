# AoE2 Campaign Report

A self-hosted AoE2:DE "last 20 games" dashboard. You type your in-game name,
it finds your profile, and it keeps a running report of your recent matches:
win rate, rating trend, civ/map/duration/opening breakdowns, and — for
matches where the replay downloaded and parsed cleanly — age-up timing,
villager counts, and first-military-unit timing pulled straight out of the
`.aoe2record` file.

It runs entirely as one small web service. No desktop app, no browser
extension, nothing needs to stay running on your computer.

## What it does

- **Search** — type a name, it searches public match records on
  aoe2insights.com and shows candidate profiles to confirm.
- **Track** — once you confirm a profile, it remembers it and refreshes
  automatically every 20 minutes in the background.
- **Replays** — for each new match it finds, it downloads the official
  replay file directly from Microsoft's replay CDN
  (`aoe.ms/replay/?gameId=...&profileId=...`) and parses it locally for
  build-order-level detail. Every downloaded replay is also available as a
  raw `.aoe2record` download from the dashboard, so you (or any other
  replay analyzer) can open it directly.
- **Dashboard** — rating trend, win/loss by civilization/map/game length,
  opening strategy tags, age-up timing trend, villager-count checkpoints,
  first-military-unit timing, and a few rule-based suggestions once there's
  enough data.

## Deploying this (Railway)

This repo is set up to deploy as-is on [Railway](https://railway.app) using
the included `Dockerfile`. Steps:

1. **Create a GitHub repo** and upload everything in this folder to it.
   Every file sits flat at the repo root (no subfolders), so on GitHub's
   web UI you can just go to your new repo → **Add file → Upload files**
   and select all the files at once (works fine from a phone too — no
   drag-and-drop of folders needed).
2. Tell Claude the repo name (`your-username/your-repo-name`) — the rest
   (creating the Railway project, attaching a persistent volume at `/data`
   so replays and the database survive redeploys, and generating a public
   URL) is done for you from there.

No environment variables are required to get it running — `DB_PATH` and
`REPLAY_DIR` already default to paths on the Railway volume, and `PORT` is
supplied by Railway automatically.

## How it stores data

Everything lives in a single SQLite database (`/data/app.db` on the
Railway volume) plus a folder of raw replay files (`/data/replays/`).
There's no external database to configure.

## Known limitation

The match-list scraping (`scraper.py`) targets aoe2insights.com's current
page structure using pattern-matching rather than a fixed API, since no
public API exists. If aoe2insights.com changes its page layout, the
scraper may need small selector updates — Railway's logs will show
`search_debug` / `matches_debug` lines that make this quick to diagnose
and fix.
