# Review Invite Service

This project is now set up for your current workflow:

- upload or paste Shopify order numbers
- check whether each order already has the `review_email_sent` tag
- send Brevo review emails only for orders that still need one

Shippo support is still in the codebase, but it is optional and not required for running the app.

## What it does now

- Sends review emails from a manual list of Shopify order numbers
- Skips orders already tagged with `review_email_sent`
- Supports dry runs before live sends
- Includes a simple admin page for manual uploads
- Still includes CSV mode and optional Shopify date-range backfill

## Required environment variables

Copy `.env.example` to `.env` and fill in:

- `SHOPIFY_SHOP_DOMAIN`
- `SHOPIFY_ACCESS_TOKEN`
- `BREVO_API_KEY`
- `BREVO_SENDER_EMAIL`
- `BREVO_SENDER_NAME`
- `TRUSTPILOT_REVIEW_URL`
- `ADMIN_TOKEN`

Optional:

- `SHIPPO_API_KEY`
- `SHIPPO_WEBHOOK_TOKEN`
- `DATABASE_PATH`

## Local commands

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the web app:

```bash
python review_invites.py serve
```

Run a CSV dry run:

```bash
python review_invites.py process-csv --input orders.csv --dry-run --output review_send_results.csv
```

Run a CSV live send:

```bash
python review_invites.py process-csv --input orders.csv --send --output review_send_results.csv
```

Optional date-range dry run:

```bash
python review_invites.py backfill-date-range --start-date 2026-03-01 --end-date 2026-03-31 --dry-run --output review_date_backfill_results.csv
```

## Admin page

Open:

```text
https://<your-domain>/admin?token=<ADMIN_TOKEN>
```

From there you can:

- paste Shopify order numbers
- upload a CSV or text file
- run a dry run
- run a live send
- see the latest run results

The manual upload flow uses the same Shopify tag check as your older script, so already-sent orders are skipped automatically.

## Endpoints

- `GET /health`
- `GET /admin?token=ADMIN_TOKEN`
- `POST /admin/upload-orders?token=ADMIN_TOKEN`
- `POST /admin/backfill-date-range?token=ADMIN_TOKEN`

Shippo-only endpoints, if you enable Shippo later:

- `POST /webhooks/shippo?token=SHIPPO_WEBHOOK_TOKEN`
- `POST /jobs/process?token=ADMIN_TOKEN`
- `GET /reports/problem-orders?token=ADMIN_TOKEN`

## Google Cloud note

With the current SQLite-based setup, the easiest Google Cloud deployment path is a small Compute Engine VM rather than Cloud Run.
