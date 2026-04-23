#!/usr/bin/env python3
"""
Cloud-ready review invite service.

Features:
- Receives Shippo tracking webhooks.
- Stores shipment state in SQLite.
- Sends Brevo review emails 24 hours after delivery by default.
- Tags Shopify orders after sending to prevent duplicates.
- Exports problem orders that are stale or overdue.
- Keeps a manual CSV mode for one-off backfills.

Examples:
python review_invites.py serve
python review_invites.py process-pending --send --output review_send_results.csv
python review_invites.py export-problems --output problematic_orders.csv
python review_invites.py process-csv --input orders.csv --output review_send_results.csv --send
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template_string, request


BATCH_CHUNK_SIZE = 250


GET_ORDERS_QUERY = """
query FindOrders($query: String!) {
  orders(first: 10, query: $query, sortKey: PROCESSED_AT, reverse: true) {
    nodes {
      id
      name
      email
      tags
      createdAt
      displayFulfillmentStatus
      fulfillments {
        trackingInfo {
          company
          number
          url
        }
      }
    }
  }
}
"""

LIST_ORDERS_QUERY = """
query ListOrders($query: String!, $after: String) {
  orders(first: 100, after: $after, query: $query, sortKey: CREATED_AT, reverse: false) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      name
      email
      tags
      createdAt
      displayFulfillmentStatus
      fulfillments {
        trackingInfo {
          company
          number
          url
        }
      }
    }
  }
}
"""

ADD_TAG_MUTATION = """
mutation AddTagToOrder($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node {
      id
    }
    userErrors {
      field
      message
    }
  }
}
"""

ADMIN_PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Review Invite Admin</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f3efe7;
        --panel: #fffdf8;
        --ink: #1f2a1f;
        --accent: #2f6b3d;
        --accent-soft: #dcead8;
        --line: #d9d0c2;
        --muted: #5f675f;
        --warn: #8a4b16;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        background: linear-gradient(180deg, #f0eadf 0%, var(--bg) 100%);
        color: var(--ink);
      }
      .wrap {
        max-width: 1180px;
        margin: 0 auto;
        padding: 28px 20px 40px;
      }
      .hero, .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 18px;
        box-shadow: 0 14px 30px rgba(49, 58, 43, 0.08);
      }
      .hero {
        padding: 24px;
        margin-bottom: 20px;
      }
      .hero h1 { margin: 0 0 8px; font-size: 32px; }
      .hero p { margin: 0; color: var(--muted); font-size: 16px; line-height: 1.5; }
      .grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 20px;
      }
      .panel { padding: 20px; }
      h2 { margin: 0 0 14px; font-size: 22px; }
      form { display: grid; gap: 12px; }
      label { display: grid; gap: 6px; font-weight: 600; }
        input[type="date"], input[type="password"], input[type="file"], textarea {
          width: 100%;
          padding: 12px 14px;
          border: 1px solid var(--line);
          border-radius: 10px;
          font: inherit;
          background: #fff;
        }
        textarea {
          min-height: 160px;
          resize: vertical;
        }
      .row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
      }
      button {
        border: 0;
        border-radius: 999px;
        padding: 11px 18px;
        cursor: pointer;
        font: inherit;
        font-weight: 700;
      }
      .primary { background: var(--accent); color: #fff; }
      .secondary { background: var(--accent-soft); color: var(--accent); }
      .message {
        margin-top: 12px;
        padding: 12px 14px;
        border-radius: 10px;
        background: #fff7df;
        color: var(--warn);
      }
      .stats {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin: 0 0 16px;
      }
      .stat {
        min-width: 120px;
        padding: 12px;
        border-radius: 12px;
        background: #f7f3eb;
        border: 1px solid var(--line);
      }
      .stat strong { display: block; font-size: 24px; }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
      }
      th, td {
        text-align: left;
        vertical-align: top;
        padding: 10px 8px;
        border-bottom: 1px solid var(--line);
      }
      th { font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); }
      .note { color: var(--muted); font-size: 14px; line-height: 1.5; }
      .empty { color: var(--muted); font-style: italic; }
      code {
        background: #f2eee6;
        padding: 2px 6px;
        border-radius: 6px;
      }
      @media (max-width: 700px) {
        .hero h1 { font-size: 26px; }
        table { display: block; overflow-x: auto; }
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <section class="hero">
        <h1>Review Invite Admin</h1>
        <p>Upload or paste Shopify order numbers for review sends, inspect the latest results, and optionally run date-range backfills later when you need them.</p>
      </section>
      <section class="grid">
        <div class="panel">
          <h2>Upload Order Numbers</h2>
          <form method="post" action="/admin/upload-orders?token={{ token|e }}" enctype="multipart/form-data">
            <label>
              Paste order numbers
              <textarea name="order_numbers_text" placeholder="1001&#10;1002&#10;1003">{{ order_numbers_text|e }}</textarea>
            </label>
            <label>
              Or upload a CSV
              <input type="file" name="orders_file" accept=".csv,.txt">
            </label>
            <div class="row">
              <button class="secondary" type="submit" name="mode" value="dry_run">Dry Run</button>
              <button class="primary" type="submit" name="mode" value="send">Send Emails</button>
            </div>
          </form>
          <p class="note">This checks each Shopify order, skips any order already tagged with <code>{{ review_tag }}</code>, and only sends to orders that still need a review email.</p>
        </div>
        <div class="panel">
          <h2>Date-Range Backfill</h2>
          <form method="post" action="/admin/backfill-date-range?token={{ token|e }}">
            <label>
              Start date
              <input type="date" name="start_date" value="{{ start_date|e }}" required>
            </label>
            <label>
              End date
              <input type="date" name="end_date" value="{{ end_date|e }}" required>
            </label>
            <div class="row">
              <button class="secondary" type="submit" name="mode" value="dry_run">Dry Run</button>
              <button class="primary" type="submit" name="mode" value="send">Send Emails</button>
            </div>
          </form>
          {% if message %}
          <div class="message">{{ message }}</div>
          {% endif %}
          <p class="note">This pulls Shopify orders created between the dates you choose, skips anything already tagged with <code>{{ review_tag }}</code>, verifies delivery in Shippo, and then sends only when Shippo says the shipment is delivered.</p>
        </div>
        <div class="panel">
          <h2>Current Problem Orders</h2>
          <div class="stats">
            <div class="stat">
              <strong>{{ problem_rows|length }}</strong>
              <span>flagged shipments</span>
            </div>
          </div>
          {% if problem_rows %}
          <table>
            <thead>
              <tr>
                <th>Order</th>
                <th>Tracking</th>
                <th>Status</th>
                <th>Last update</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {% for row in problem_rows %}
              <tr>
                <td>{{ row.shopify_order_name or row.order_number }}</td>
                <td>{{ row.carrier }}/{{ row.tracking_number }}</td>
                <td>{{ row.last_tracking_status }}</td>
                <td>{{ row.last_tracking_update_at_display }}</td>
                <td>{{ row.problem_reason }}</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
          {% else %}
          <p class="empty">No problem orders are currently flagged.</p>
          {% endif %}
        </div>
      </section>
      <section class="panel" style="margin-top: 20px;">
        <h2>Latest Manual Batch</h2>
        {% if latest_batch_job %}
        <div class="stats">
          <div class="stat">
            <strong>{{ latest_batch_job.status }}</strong>
            <span>status</span>
          </div>
          <div class="stat">
            <strong>{{ latest_batch_job.processed_count }}/{{ latest_batch_job.total_count }}</strong>
            <span>processed</span>
          </div>
          <div class="stat">
            <strong>{{ latest_batch_job.mode }}</strong>
            <span>mode</span>
          </div>
          <div class="stat">
            <strong>{{ latest_batch_job.batch_size }}</strong>
            <span>chunk size</span>
          </div>
        </div>
        {% if latest_batch_counts %}
        <div class="stats">
          {% for row in latest_batch_counts %}
          <div class="stat">
            <strong>{{ row.count }}</strong>
            <span>{{ row.status }}</span>
          </div>
          {% endfor %}
        </div>
        {% endif %}
        {% if latest_batch_job.error_detail %}
        <div class="message">{{ latest_batch_job.error_detail }}</div>
        {% endif %}
        {% if latest_batch_results %}
        <p class="note">Showing the most recent 200 result rows. Refresh this page to update progress while a job is running.</p>
        <table>
          <thead>
            <tr>
              <th>Order</th>
              <th>Email</th>
              <th>Status</th>
              <th>Detail</th>
            </tr>
          </thead>
          <tbody>
            {% for row in latest_batch_results %}
            <tr>
              <td>{{ row.shopify_order_name or row.input_order_number }}</td>
              <td>{{ row.email }}</td>
              <td>{{ row.status }}</td>
              <td>{{ row.detail }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="empty">No result rows have been written for this batch yet.</p>
        {% endif %}
        {% else %}
        <p class="empty">No manual batch has been started yet.</p>
        {% endif %}
      </section>
      <section class="panel" style="margin-top: 20px;">
        <h2>Last Backfill Run</h2>
        {% if last_run %}
        <div class="stats">
          <div class="stat">
            <strong>{{ last_run.start_date }}</strong>
            <span>start date</span>
          </div>
          <div class="stat">
            <strong>{{ last_run.end_date }}</strong>
            <span>end date</span>
          </div>
          <div class="stat">
            <strong>{{ last_run.mode }}</strong>
            <span>run mode</span>
          </div>
          <div class="stat">
            <strong>{{ last_run.total }}</strong>
            <span>orders checked</span>
          </div>
        </div>
        <p class="note">Run completed at {{ last_run.completed_at_display }}.</p>
        {% if last_run.results %}
        <table>
          <thead>
            <tr>
              <th>Order</th>
              <th>Email</th>
              <th>Status</th>
              <th>Detail</th>
            </tr>
          </thead>
          <tbody>
            {% for row in last_run.results %}
            <tr>
              <td>{{ row.shopify_order_name or row.input_order_number }}</td>
              <td>{{ row.email }}</td>
              <td>{{ row.status }}</td>
              <td>{{ row.detail }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
        <p class="empty">That run returned no rows.</p>
        {% endif %}
        {% else %}
        <p class="empty">No date-range backfill has been run from the admin page yet.</p>
        {% endif %}
      </section>
    </div>
  </body>
</html>
"""


@dataclass
class AppConfig:
    shopify_shop_domain: str
    shopify_access_token: str
    shopify_api_version: str
    brevo_api_key: str
    brevo_sender_email: str
    brevo_sender_name: str
    trustpilot_review_url: str
    review_tag: str
    shop_name: str
    pause_seconds: float
    email_subject_template: str
    database_path: str
    shippo_api_key: str
    shippo_webhook_token: str
    admin_token: str
    review_delay_hours: int
    stale_tracking_hours: int
    delivery_max_age_days: int
    port: int


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_date_input(value: str, end_of_day: bool = False) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("Date value is required.")

    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise RuntimeError(f"Invalid date '{raw}'. Use YYYY-MM-DD.") from exc

    if end_of_day:
        parsed = parsed.replace(hour=23, minute=59, second=59)

    return parsed.replace(tzinfo=timezone.utc)


def format_admin_datetime(value: Optional[str]) -> str:
    parsed = parse_datetime(value)
    if not parsed:
        return value or ""
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def build_shopify_date_range_query(start_date: str, end_date: str) -> str:
    start = isoformat_utc(parse_date_input(start_date, end_of_day=False))
    end = isoformat_utc(parse_date_input(end_date, end_of_day=True))
    if parse_datetime(start) > parse_datetime(end):
        raise RuntimeError("Start date must be on or before end date.")

    return (
        f"created_at:>='{start}' "
        f"created_at:<='{end}' "
        "status:any"
    )


def parse_order_numbers_text(raw_text: str) -> List[str]:
    values: List[str] = []
    seen: set[str] = set()
    for line in str(raw_text or "").replace(",", "\n").splitlines():
        normalized = normalize_order_number(line)
        if not normalized or normalized.lower() == "order_number":
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return values


def normalize_order_number(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if value.startswith("#"):
        value = value[1:]
    return value.strip()


def normalize_order_name(order_name: str) -> str:
    return normalize_order_number(order_name)


def has_tag(tags: Iterable[str], target_tag: str) -> bool:
    lowered_target = target_tag.strip().lower()
    return any(str(tag).strip().lower() == lowered_target for tag in tags)


def build_email_subject(template: str, order_name: str, shop_name: str) -> str:
    display_order_name = order_name if order_name.startswith("#") else f"#{order_name}"
    return template.format(order_name=display_order_name, shop_name=shop_name)


def build_email_html(order_name: str, trustpilot_review_url: str, shop_name: str) -> str:
    display_order_name = order_name if order_name.startswith("#") else f"#{order_name}"
    safe_order_name = html.escape(display_order_name)
    safe_shop_name = html.escape(shop_name)
    safe_url = html.escape(trustpilot_review_url, quote=True)

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>We'd love your feedback</title>
  </head>
  <body style="margin:0;padding:0;background-color:#f6f6f6;font-family:Arial,Helvetica,sans-serif;color:#222;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#f6f6f6;padding:24px 0;">
      <tr>
        <td align="center">
          <table role="presentation" width="600" cellspacing="0" cellpadding="0" style="max-width:600px;width:100%;background-color:#ffffff;border-collapse:collapse;border-radius:8px;overflow:hidden;">
            <tr>
              <td style="padding:32px;">
                <p style="margin:0 0 16px 0;font-size:16px;line-height:1.6;">Hi Fellow Gardener,</p>
                <p style="margin:0 0 16px 0;font-size:16px;line-height:1.6;">
                  Thank you so much for choosing {safe_shop_name}! We hope you are absolutely loving your recent order
                  <strong>{safe_order_name}</strong>.
                </p>
                <p style="margin:0 0 24px 0;font-size:16px;line-height:1.6;">
                  As a small business, we rely heavily on word-of-mouth. If you have a spare two minutes, we would be incredibly
                  grateful if you could share your honest thoughts on Trustpilot about your experience with us.
                </p>
                <table role="presentation" cellspacing="0" cellpadding="0" style="margin:0 0 24px 0;">
                  <tr>
                    <td align="center" style="border-radius:6px;">
                      <a href="{safe_url}" style="display:inline-block;padding:14px 22px;font-size:16px;line-height:1;color:#ffffff;background-color:#111111;text-decoration:none;border-radius:6px;">Leave a review</a>
                    </td>
                  </tr>
                </table>
                <p style="margin:0 0 12px 0;font-size:14px;line-height:1.6;color:#555;">
                  If the button does not work, copy and paste this link into your browser:
                </p>
                <p style="margin:0 0 24px 0;font-size:14px;line-height:1.6;word-break:break-all;">
                  <a href="{safe_url}" style="color:#111111;">{safe_url}</a>
                </p>
                <p style="margin:0;font-size:16px;line-height:1.6;">
                  Thank you again for your support,<br>
                  {safe_shop_name}
                </p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""


def parse_metadata(raw_metadata: Any) -> Dict[str, str]:
    if isinstance(raw_metadata, dict):
        return {
            str(key).strip(): str(value).strip()
            for key, value in raw_metadata.items()
            if str(key).strip() and str(value).strip()
        }

    if raw_metadata is None:
        return {}

    text = str(raw_metadata).strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, dict):
        return {
            str(key).strip(): str(value).strip()
            for key, value in parsed.items()
            if str(key).strip() and str(value).strip()
        }

    metadata: Dict[str, str] = {}
    for part in text.split(","):
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        clean_key = key.strip()
        clean_value = value.strip()
        if clean_key and clean_value:
            metadata[clean_key] = clean_value
    return metadata


def ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        ensure_parent_dir(path)
        self._init_db()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shipments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tracking_number TEXT NOT NULL,
                    carrier TEXT NOT NULL,
                    order_number TEXT,
                    shopify_order_id TEXT,
                    shopify_order_name TEXT,
                    customer_email TEXT,
                    last_tracking_status TEXT,
                    last_tracking_substatus TEXT,
                    last_tracking_update_at TEXT,
                    delivered_at TEXT,
                    review_email_sent_at TEXT,
                    review_email_status TEXT,
                    review_email_message_id TEXT,
                    problem_flag TEXT,
                    problem_detail TEXT,
                    latest_payload TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(tracking_number, carrier)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_count INTEGER NOT NULL,
                    processed_count INTEGER NOT NULL,
                    batch_size INTEGER NOT NULL,
                    order_numbers_json TEXT NOT NULL,
                    error_detail TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batch_job_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    input_order_number TEXT,
                    normalized_order_number TEXT,
                    shopify_order_name TEXT,
                    email TEXT,
                    status TEXT,
                    detail TEXT,
                    brevo_message_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES batch_jobs(id)
                )
                """
            )

    def create_batch_job(
        self,
        mode: str,
        order_numbers: List[str],
        batch_size: int,
    ) -> str:
        job_id = uuid.uuid4().hex
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO batch_jobs (
                    id, mode, status, total_count, processed_count, batch_size,
                    order_numbers_json, error_detail, created_at, started_at,
                    completed_at, updated_at
                ) VALUES (?, ?, 'pending', ?, 0, ?, ?, NULL, ?, NULL, NULL, ?)
                """,
                (
                    job_id,
                    mode,
                    len(order_numbers),
                    batch_size,
                    json.dumps(order_numbers),
                    now,
                    now,
                ),
            )
        return job_id

    def get_batch_job(self, job_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM batch_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()

    def get_latest_batch_job(self) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM batch_jobs
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def get_active_batch_job(self) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM batch_jobs
                WHERE status IN ('pending', 'running')
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def update_batch_job(
        self,
        job_id: str,
        status: str,
        processed_count: Optional[int] = None,
        error_detail: Optional[str] = None,
        started_at: Optional[str] = None,
        completed_at: Optional[str] = None,
    ) -> None:
        existing = self.get_batch_job(job_id)
        if not existing:
            return

        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE batch_jobs
                SET status = ?,
                    processed_count = ?,
                    error_detail = ?,
                    started_at = COALESCE(?, started_at),
                    completed_at = COALESCE(?, completed_at),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    processed_count if processed_count is not None else existing["processed_count"],
                    error_detail if error_detail is not None else existing["error_detail"],
                    started_at,
                    completed_at,
                    now,
                    job_id,
                ),
            )

    def append_batch_results(self, job_id: str, results: List[Dict[str, str]]) -> None:
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO batch_job_results (
                    job_id, input_order_number, normalized_order_number,
                    shopify_order_name, email, status, detail, brevo_message_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        job_id,
                        row.get("input_order_number", ""),
                        row.get("normalized_order_number", ""),
                        row.get("shopify_order_name", ""),
                        row.get("email", ""),
                        row.get("status", ""),
                        row.get("detail", ""),
                        row.get("brevo_message_id", ""),
                        now,
                    )
                    for row in results
                ],
            )

    def list_batch_results(self, job_id: str, limit: int = 200) -> List[Dict[str, str]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT input_order_number, normalized_order_number, shopify_order_name,
                       email, status, detail, brevo_message_id
                FROM batch_job_results
                WHERE job_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (job_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_batch_result_counts(self, job_id: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM batch_job_results
                WHERE job_id = ?
                GROUP BY status
                ORDER BY count DESC
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_shipment(self, shipment: Dict[str, Any]) -> None:
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            existing = conn.execute(
                """
                SELECT * FROM shipments
                WHERE tracking_number = ? AND carrier = ?
                """,
                (shipment["tracking_number"], shipment["carrier"]),
            ).fetchone()

            if existing:
                merged = dict(existing)
                for key, value in shipment.items():
                    if value not in (None, ""):
                        merged[key] = value
                merged["updated_at"] = now
                conn.execute(
                    """
                    UPDATE shipments
                    SET order_number = ?, shopify_order_id = ?, shopify_order_name = ?,
                        customer_email = ?, last_tracking_status = ?, last_tracking_substatus = ?,
                        last_tracking_update_at = ?, delivered_at = ?, problem_flag = ?,
                        problem_detail = ?, latest_payload = ?, updated_at = ?
                    WHERE tracking_number = ? AND carrier = ?
                    """,
                    (
                        merged.get("order_number"),
                        merged.get("shopify_order_id"),
                        merged.get("shopify_order_name"),
                        merged.get("customer_email"),
                        merged.get("last_tracking_status"),
                        merged.get("last_tracking_substatus"),
                        merged.get("last_tracking_update_at"),
                        merged.get("delivered_at"),
                        merged.get("problem_flag"),
                        merged.get("problem_detail"),
                        merged.get("latest_payload"),
                        merged["updated_at"],
                        shipment["tracking_number"],
                        shipment["carrier"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO shipments (
                        tracking_number, carrier, order_number, shopify_order_id,
                        shopify_order_name, customer_email, last_tracking_status,
                        last_tracking_substatus, last_tracking_update_at, delivered_at,
                        review_email_sent_at, review_email_status, review_email_message_id,
                        problem_flag, problem_detail, latest_payload, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?)
                    """,
                    (
                        shipment["tracking_number"],
                        shipment["carrier"],
                        shipment.get("order_number"),
                        shipment.get("shopify_order_id"),
                        shipment.get("shopify_order_name"),
                        shipment.get("customer_email"),
                        shipment.get("last_tracking_status"),
                        shipment.get("last_tracking_substatus"),
                        shipment.get("last_tracking_update_at"),
                        shipment.get("delivered_at"),
                        shipment.get("problem_flag"),
                        shipment.get("problem_detail"),
                        shipment.get("latest_payload"),
                        now,
                        now,
                    ),
                )

    def get_due_review_shipments(self, review_delay_hours: int) -> List[sqlite3.Row]:
        threshold = isoformat_utc(utc_now() - timedelta(hours=review_delay_hours))
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM shipments
                WHERE delivered_at IS NOT NULL
                  AND delivered_at <= ?
                  AND review_email_sent_at IS NULL
                ORDER BY delivered_at ASC
                """,
                (threshold,),
            ).fetchall()

    def mark_review_result(
        self,
        shipment_id: int,
        status: str,
        sent_at: Optional[str],
        message_id: str,
        detail: str,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE shipments
                SET review_email_status = ?, review_email_sent_at = ?, review_email_message_id = ?,
                    problem_detail = CASE
                        WHEN ? = 'sent' THEN NULL
                        ELSE COALESCE(problem_detail, ?)
                    END,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, sent_at, message_id, status, detail, isoformat_utc(utc_now()), shipment_id),
            )

    def list_problem_shipments(
        self,
        stale_tracking_hours: int,
        delivery_max_age_days: int,
    ) -> List[Dict[str, str]]:
        now = utc_now()
        stale_cutoff = now - timedelta(hours=stale_tracking_hours)
        overdue_cutoff = now - timedelta(days=delivery_max_age_days)
        results: List[Dict[str, str]] = []

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM shipments
                WHERE review_email_sent_at IS NULL
                ORDER BY updated_at ASC
                """
            ).fetchall()

        for row in rows:
            delivered_at = parse_datetime(row["delivered_at"])
            last_update = parse_datetime(row["last_tracking_update_at"])
            created_at = parse_datetime(row["created_at"])

            reason = ""
            if delivered_at is None and last_update and last_update <= stale_cutoff:
                reason = f"No tracking updates for at least {stale_tracking_hours} hours."
            elif delivered_at is None and created_at and created_at <= overdue_cutoff:
                reason = f"Still not delivered after at least {delivery_max_age_days} days."

            if not reason:
                continue

            results.append(
                {
                    "order_number": row["order_number"] or "",
                    "shopify_order_name": row["shopify_order_name"] or "",
                    "customer_email": row["customer_email"] or "",
                    "tracking_number": row["tracking_number"] or "",
                    "carrier": row["carrier"] or "",
                    "last_tracking_status": row["last_tracking_status"] or "",
                    "last_tracking_update_at": row["last_tracking_update_at"] or "",
                    "delivered_at": row["delivered_at"] or "",
                    "problem_reason": reason,
                }
            )

        return results


class ReviewInviteSender:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.shopify_graphql_url = (
            f"https://{config.shopify_shop_domain}/admin/api/"
            f"{config.shopify_api_version}/graphql.json"
        )
        self.brevo_url = "https://api.brevo.com/v3/smtp/email"
        self.session = requests.Session()

    def shopify_graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.config.shopify_access_token,
        }
        response = self.session.post(
            self.shopify_graphql_url,
            headers=headers,
            json={"query": query, "variables": variables},
            timeout=60,
        )

        try:
            response_json = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Shopify returned a non-JSON response (HTTP {response.status_code})."
            ) from exc

        if not response.ok:
            raise RuntimeError(f"Shopify API error {response.status_code}: {response_json}")

        if "errors" in response_json:
            raise RuntimeError(f"Shopify GraphQL errors: {response_json['errors']}")

        return response_json["data"]

    def find_order_by_number(self, order_number: str) -> Optional[Dict[str, Any]]:
        normalized_input = normalize_order_number(order_number)
        data = self.shopify_graphql(GET_ORDERS_QUERY, {"query": f"name:{normalized_input}"})
        candidates = data["orders"]["nodes"]

        exact_matches = [
            order
            for order in candidates
            if normalize_order_name(order["name"]) == normalized_input
        ]

        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            raise RuntimeError(
                f"Multiple exact Shopify matches were found for order number {order_number}."
            )
        return None

    def list_orders(self, query: str) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []
        after: Optional[str] = None

        while True:
            data = self.shopify_graphql(LIST_ORDERS_QUERY, {"query": query, "after": after})
            connection = data["orders"]
            orders.extend(connection["nodes"])
            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            after = page_info["endCursor"]

        return orders

    def send_brevo_email(self, to_email: str, subject: str, html_content: str) -> str:
        response = self.session.post(
            self.brevo_url,
            headers={
                "accept": "application/json",
                "api-key": self.config.brevo_api_key,
                "content-type": "application/json",
            },
            json={
                "sender": {
                    "email": self.config.brevo_sender_email,
                    "name": self.config.brevo_sender_name,
                },
                "to": [{"email": to_email}],
                "subject": subject,
                "htmlContent": html_content,
            },
            timeout=60,
        )

        try:
            response_json = response.json() if response.text else {}
        except ValueError:
            response_json = {"raw": response.text}

        if not response.ok:
            raise RuntimeError(f"Brevo API error {response.status_code}: {response_json}")

        return str(response_json.get("messageId", ""))

    def add_order_tag(self, order_id: str, tag: str) -> None:
        data = self.shopify_graphql(ADD_TAG_MUTATION, {"id": order_id, "tags": [tag]})
        user_errors = data["tagsAdd"]["userErrors"]
        if user_errors:
            raise RuntimeError(f"Shopify tagsAdd returned errors: {user_errors}")

    def get_shippo_tracking_status(self, carrier: str, tracking_number: str) -> Dict[str, Any]:
        if not self.config.shippo_api_key:
            raise RuntimeError("SHIPPO_API_KEY is not configured.")
        carrier_token = str(carrier or "").strip().lower()
        tracking_value = str(tracking_number or "").strip()
        if not carrier_token or not tracking_value:
            raise RuntimeError("Carrier and tracking number are required for Shippo tracking lookup.")

        response = self.session.get(
            f"https://api.goshippo.com/tracks/{carrier_token}/{tracking_value}",
            headers={
                "Authorization": f"ShippoToken {self.config.shippo_api_key}",
            },
            timeout=60,
        )

        try:
            response_json = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Shippo returned a non-JSON response (HTTP {response.status_code})."
            ) from exc

        if not response.ok:
            raise RuntimeError(f"Shippo API error {response.status_code}: {response_json}")

        return response_json


def load_config() -> AppConfig:
    load_dotenv()

    required_vars = [
        "SHOPIFY_SHOP_DOMAIN",
        "SHOPIFY_ACCESS_TOKEN",
        "BREVO_API_KEY",
        "BREVO_SENDER_EMAIL",
        "BREVO_SENDER_NAME",
        "TRUSTPILOT_REVIEW_URL",
        "ADMIN_TOKEN",
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    default_db_path = str(Path(__file__).resolve().parent / "data" / "review_invites.db")

    return AppConfig(
        shopify_shop_domain=os.environ["SHOPIFY_SHOP_DOMAIN"].strip(),
        shopify_access_token=os.environ["SHOPIFY_ACCESS_TOKEN"].strip(),
        shopify_api_version=os.getenv("SHOPIFY_API_VERSION", "2026-01").strip(),
        brevo_api_key=os.environ["BREVO_API_KEY"].strip(),
        brevo_sender_email=os.environ["BREVO_SENDER_EMAIL"].strip(),
        brevo_sender_name=os.environ["BREVO_SENDER_NAME"].strip(),
        trustpilot_review_url=os.environ["TRUSTPILOT_REVIEW_URL"].strip(),
        review_tag=os.getenv("REVIEW_TAG", "review_email_sent").strip(),
        shop_name=os.getenv("SHOP_NAME", os.getenv("BREVO_SENDER_NAME", "Our store")).strip(),
        pause_seconds=float(os.getenv("PAUSE_SECONDS", "0.25")),
        email_subject_template=os.getenv(
            "EMAIL_SUBJECT_TEMPLATE",
            "We'd love your feedback on your order {order_name}",
        ).strip(),
        database_path=os.getenv("DATABASE_PATH", default_db_path).strip(),
        shippo_api_key=os.getenv("SHIPPO_API_KEY", "").strip(),
        shippo_webhook_token=os.getenv("SHIPPO_WEBHOOK_TOKEN", "").strip(),
        admin_token=os.environ["ADMIN_TOKEN"].strip(),
        review_delay_hours=int(os.getenv("REVIEW_DELAY_HOURS", "24")),
        stale_tracking_hours=int(os.getenv("STALE_TRACKING_HOURS", "72")),
        delivery_max_age_days=int(os.getenv("DELIVERY_MAX_AGE_DAYS", "10")),
        port=int(os.getenv("PORT", "8000")),
    )


def read_order_numbers(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path):
        raise RuntimeError(f"Input CSV file was not found: {csv_path}")

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise RuntimeError("The input CSV has no header row.")

        normalized_headers = {
            header.strip().lower(): header for header in reader.fieldnames if header
        }
        if "order_number" not in normalized_headers:
            raise RuntimeError("The input CSV must contain a column named order_number.")

        source_header = normalized_headers["order_number"]
        values = [str(row.get(source_header, "") or "").strip() for row in reader]

    values = [value for value in values if value]
    if not values:
        raise RuntimeError("No order numbers were found in the input CSV.")
    return values


def write_results_csv(output_path: str, results: List[Dict[str, str]]) -> None:
    if not results:
        results = []

    fieldnames = [
        "input_order_number",
        "normalized_order_number",
        "shopify_order_name",
        "email",
        "status",
        "detail",
        "brevo_message_id",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def write_problem_csv(output_path: str, rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "order_number",
        "shopify_order_name",
        "customer_email",
        "tracking_number",
        "carrier",
        "last_tracking_status",
        "last_tracking_update_at",
        "delivered_at",
        "problem_reason",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_tracking_candidates(order: Dict[str, Any]) -> List[Tuple[str, str]]:
    candidates: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    fulfillments = order.get("fulfillments") or []
    if isinstance(fulfillments, dict):
        fulfillments = fulfillments.get("nodes", [])

    for fulfillment in fulfillments:
        for tracking in fulfillment.get("trackingInfo") or []:
            carrier = str(tracking.get("company") or "").strip().lower()
            tracking_number = str(tracking.get("number") or "").strip()
            if not carrier or not tracking_number:
                continue

            candidate = (carrier, tracking_number)
            if candidate in seen:
                continue

            seen.add(candidate)
            candidates.append(candidate)

    return candidates


def process_orders(
    sender: ReviewInviteSender,
    order_numbers: List[str],
    dry_run: bool,
) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []

    for raw_order_number in order_numbers:
        normalized_input = normalize_order_number(raw_order_number)
        result_row: Dict[str, str] = {
            "input_order_number": raw_order_number,
            "normalized_order_number": normalized_input,
            "shopify_order_name": "",
            "email": "",
            "status": "",
            "detail": "",
            "brevo_message_id": "",
        }

        if not normalized_input:
            result_row["status"] = "error"
            result_row["detail"] = "Blank order number after normalization."
            results.append(result_row)
            continue

        try:
            order = sender.find_order_by_number(normalized_input)
            if not order:
                result_row["status"] = "not_found"
                result_row["detail"] = "No exact Shopify order match found."
                results.append(result_row)
                continue

            result_row["shopify_order_name"] = order.get("name", "")
            result_row["email"] = order.get("email") or ""

            current_tags = order.get("tags") or []
            if has_tag(current_tags, sender.config.review_tag):
                result_row["status"] = "skipped_already_tagged"
                result_row["detail"] = (
                    f"Order already has tag '{sender.config.review_tag}'."
                )
                results.append(result_row)
                continue

            order_email = (order.get("email") or "").strip()
            if not order_email:
                result_row["status"] = "skipped_no_email"
                result_row["detail"] = "Order has no email address in Shopify."
                results.append(result_row)
                continue

            subject = build_email_subject(
                sender.config.email_subject_template,
                order.get("name", normalized_input),
                sender.config.shop_name,
            )
            html_content = build_email_html(
                order.get("name", normalized_input),
                sender.config.trustpilot_review_url,
                sender.config.shop_name,
            )

            if dry_run:
                result_row["status"] = "dry_run_would_send"
                result_row["detail"] = "Dry run enabled. No email sent and no tag added."
                results.append(result_row)
                continue

            message_id = sender.send_brevo_email(order_email, subject, html_content)
            sender.add_order_tag(order["id"], sender.config.review_tag)

            result_row["status"] = "sent"
            result_row["detail"] = (
                f"Email sent through Brevo and tag '{sender.config.review_tag}' added."
            )
            result_row["brevo_message_id"] = message_id
            results.append(result_row)
            time.sleep(sender.config.pause_seconds)
        except Exception as exc:
            result_row["status"] = "error"
            result_row["detail"] = str(exc)
            results.append(result_row)

    return results


def backfill_delivered_orders(
    sender: ReviewInviteSender,
    order_numbers: List[str],
    dry_run: bool,
) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []

    for raw_order_number in order_numbers:
        normalized_input = normalize_order_number(raw_order_number)
        result_row: Dict[str, str] = {
            "input_order_number": raw_order_number,
            "normalized_order_number": normalized_input,
            "shopify_order_name": "",
            "email": "",
            "status": "",
            "detail": "",
            "brevo_message_id": "",
        }

        if not normalized_input:
            result_row["status"] = "error"
            result_row["detail"] = "Blank order number after normalization."
            results.append(result_row)
            continue

        try:
            order = sender.find_order_by_number(normalized_input)
            if not order:
                result_row["status"] = "not_found"
                result_row["detail"] = "No exact Shopify order match found."
                results.append(result_row)
                continue

            result_row["shopify_order_name"] = order.get("name", "")
            result_row["email"] = (order.get("email") or "").strip()

            current_tags = order.get("tags") or []
            if has_tag(current_tags, sender.config.review_tag):
                result_row["status"] = "skipped_already_tagged"
                result_row["detail"] = (
                    f"Order already has tag '{sender.config.review_tag}'."
                )
                results.append(result_row)
                continue

            order_email = result_row["email"]
            if not order_email:
                result_row["status"] = "skipped_no_email"
                result_row["detail"] = "Order has no email address in Shopify."
                results.append(result_row)
                continue

            tracking_candidates = extract_tracking_candidates(order)
            if not tracking_candidates:
                result_row["status"] = "skipped_no_tracking"
                result_row["detail"] = "No tracking number and carrier were found on the Shopify order."
                results.append(result_row)
                continue

            delivered_tracking: Optional[Tuple[str, str, Dict[str, Any]]] = None
            lookup_errors: List[str] = []

            for carrier, tracking_number in tracking_candidates:
                try:
                    tracking_response = sender.get_shippo_tracking_status(carrier, tracking_number)
                    tracking_status = tracking_response.get("tracking_status") or {}
                    status_value = str(tracking_status.get("status") or "").strip().upper()
                    if status_value == "DELIVERED":
                        delivered_tracking = (carrier, tracking_number, tracking_response)
                        break
                except Exception as exc:
                    lookup_errors.append(f"{carrier}/{tracking_number}: {exc}")

            if delivered_tracking is None:
                if lookup_errors and len(lookup_errors) == len(tracking_candidates):
                    result_row["status"] = "error"
                    result_row["detail"] = "Shippo tracking lookups failed: " + " | ".join(lookup_errors)
                else:
                    result_row["status"] = "skipped_not_delivered"
                    result_row["detail"] = (
                        "Shippo did not report any order tracking number as DELIVERED yet."
                    )
                results.append(result_row)
                continue

            carrier, tracking_number, tracking_response = delivered_tracking
            tracking_status = tracking_response.get("tracking_status") or {}
            delivered_at = str(tracking_status.get("status_date") or "").strip()

            subject = build_email_subject(
                sender.config.email_subject_template,
                order.get("name", normalized_input),
                sender.config.shop_name,
            )
            html_content = build_email_html(
                order.get("name", normalized_input),
                sender.config.trustpilot_review_url,
                sender.config.shop_name,
            )

            if dry_run:
                result_row["status"] = "dry_run_would_send"
                result_row["detail"] = (
                    f"Shippo confirmed delivery via {carrier}/{tracking_number}"
                    + (f" at {delivered_at}." if delivered_at else ".")
                )
                results.append(result_row)
                continue

            message_id = sender.send_brevo_email(order_email, subject, html_content)
            sender.add_order_tag(order["id"], sender.config.review_tag)

            result_row["status"] = "sent"
            result_row["detail"] = (
                f"Backfill email sent after Shippo delivery confirmation via {carrier}/{tracking_number}."
            )
            result_row["brevo_message_id"] = message_id
            results.append(result_row)
            time.sleep(sender.config.pause_seconds)
        except Exception as exc:
            result_row["status"] = "error"
            result_row["detail"] = str(exc)
            results.append(result_row)

    return results


def backfill_delivered_orders_from_shopify_date_range(
    sender: ReviewInviteSender,
    start_date: str,
    end_date: str,
    dry_run: bool,
) -> List[Dict[str, str]]:
    query = build_shopify_date_range_query(start_date, end_date)
    orders = sender.list_orders(query)

    candidate_order_numbers = [
        normalize_order_name(order.get("name", ""))
        for order in orders
        if normalize_order_name(order.get("name", ""))
    ]

    return backfill_delivered_orders(sender, candidate_order_numbers, dry_run=dry_run)


def extract_shippo_tracking_details(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        data = payload

    tracking_status = data.get("tracking_status")
    if not isinstance(tracking_status, dict):
        tracking_status = {}

    metadata = parse_metadata(data.get("metadata") or payload.get("metadata"))
    tracking_number = (
        data.get("tracking_number")
        or tracking_status.get("tracking_number")
        or metadata.get("tracking_number")
        or ""
    )
    carrier = (
        data.get("carrier")
        or tracking_status.get("carrier")
        or metadata.get("carrier")
        or "unknown"
    )

    status = tracking_status.get("status") or data.get("tracking_status") or ""
    if isinstance(status, dict):
        status = status.get("status", "")

    status_date = (
        tracking_status.get("status_date")
        or data.get("status_date")
        or payload.get("status_date")
        or isoformat_utc(utc_now())
    )

    delivered_at = None
    if str(status).strip().upper() == "DELIVERED":
        delivered_at = status_date
    elif data.get("eta"):
        delivered_at = None

    order_number = (
        metadata.get("order_number")
        or metadata.get("order")
        or metadata.get("shopify_order_name")
        or ""
    )

    return {
        "tracking_number": str(tracking_number).strip(),
        "carrier": str(carrier).strip().lower(),
        "order_number": normalize_order_number(order_number),
        "shopify_order_id": str(metadata.get("shopify_order_id", "")).strip(),
        "shopify_order_name": str(
            metadata.get("shopify_order_name") or metadata.get("order_name") or order_number
        ).strip(),
        "customer_email": str(metadata.get("customer_email", "")).strip(),
        "last_tracking_status": str(status).strip(),
        "last_tracking_substatus": str(
            tracking_status.get("substatus") or tracking_status.get("status_details") or ""
        ).strip(),
        "last_tracking_update_at": str(status_date).strip(),
        "delivered_at": str(delivered_at).strip() if delivered_at else "",
        "problem_flag": "",
        "problem_detail": "",
        "latest_payload": json.dumps(payload, sort_keys=True),
    }


def process_due_reviews(
    config: AppConfig,
    db: Database,
    sender: ReviewInviteSender,
    dry_run: bool,
) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    due_rows = db.get_due_review_shipments(config.review_delay_hours)

    for row in due_rows:
        order_number = row["order_number"] or row["shopify_order_name"] or ""
        normalized_input = normalize_order_number(order_number)
        result_row = {
            "input_order_number": order_number,
            "normalized_order_number": normalized_input,
            "shopify_order_name": row["shopify_order_name"] or "",
            "email": row["customer_email"] or "",
            "status": "",
            "detail": "",
            "brevo_message_id": "",
        }

        try:
            if not normalized_input:
                result_row["status"] = "skipped_missing_order_number"
                result_row["detail"] = (
                    "Shippo webhook did not include order metadata. Add order metadata to Shippo tracking registrations."
                )
                results.append(result_row)
                continue

            order = sender.find_order_by_number(normalized_input)
            if not order:
                result_row["status"] = "not_found"
                result_row["detail"] = "No exact Shopify order match found."
                results.append(result_row)
                continue

            result_row["shopify_order_name"] = order.get("name", "")
            result_row["email"] = (order.get("email") or row["customer_email"] or "").strip()

            current_tags = order.get("tags") or []
            if has_tag(current_tags, config.review_tag):
                result_row["status"] = "skipped_already_tagged"
                result_row["detail"] = f"Order already has tag '{config.review_tag}'."
                db.mark_review_result(
                    row["id"],
                    "already_tagged",
                    isoformat_utc(utc_now()),
                    "",
                    result_row["detail"],
                )
                results.append(result_row)
                continue

            order_email = result_row["email"]
            if not order_email:
                result_row["status"] = "skipped_no_email"
                result_row["detail"] = "Order has no email address in Shopify."
                results.append(result_row)
                continue

            subject = build_email_subject(
                config.email_subject_template,
                order.get("name", normalized_input),
                config.shop_name,
            )
            html_content = build_email_html(
                order.get("name", normalized_input),
                config.trustpilot_review_url,
                config.shop_name,
            )

            if dry_run:
                result_row["status"] = "dry_run_would_send"
                result_row["detail"] = "Dry run enabled. No email sent and no tag added."
                results.append(result_row)
                continue

            message_id = sender.send_brevo_email(order_email, subject, html_content)
            sender.add_order_tag(order["id"], config.review_tag)
            sent_at = isoformat_utc(utc_now())
            db.mark_review_result(
                row["id"],
                "sent",
                sent_at,
                message_id,
                f"Email sent and tag '{config.review_tag}' added.",
            )

            result_row["status"] = "sent"
            result_row["detail"] = f"Email sent and tag '{config.review_tag}' added."
            result_row["brevo_message_id"] = message_id
            results.append(result_row)
            time.sleep(config.pause_seconds)
        except Exception as exc:
            result_row["status"] = "error"
            result_row["detail"] = str(exc)
            results.append(result_row)

    return results


def print_summary(results: List[Dict[str, str]]) -> None:
    counts: Dict[str, int] = {}
    for row in results:
        counts[row["status"]] = counts.get(row["status"], 0) + 1

    print("\nRun summary")
    print("-----------")
    for status in sorted(counts):
        print(f"{status}: {counts[status]}")


def prepare_problem_rows_for_display(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    prepared: List[Dict[str, str]] = []
    for row in rows:
        display_row = dict(row)
        display_row["last_tracking_update_at_display"] = format_admin_datetime(
            row.get("last_tracking_update_at")
        )
        prepared.append(display_row)
    return prepared


def build_last_run_payload(
    start_date: str,
    end_date: str,
    mode: str,
    results: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "start_date": start_date,
        "end_date": end_date,
        "mode": mode,
        "total": len(results),
        "completed_at": isoformat_utc(utc_now()),
        "completed_at_display": format_admin_datetime(isoformat_utc(utc_now())),
        "results": results[:200],
    }


def get_latest_batch_context(db: Database) -> Dict[str, Any]:
    latest_job = db.get_latest_batch_job()
    if not latest_job:
        return {
            "latest_batch_job": None,
            "latest_batch_results": [],
            "latest_batch_counts": [],
        }

    return {
        "latest_batch_job": latest_job,
        "latest_batch_results": db.list_batch_results(latest_job["id"], limit=200),
        "latest_batch_counts": db.list_batch_result_counts(latest_job["id"]),
    }


def run_manual_batch_job(config: AppConfig, job_id: str) -> None:
    db = Database(config.database_path)
    sender = ReviewInviteSender(config)
    job = db.get_batch_job(job_id)
    if not job:
        return

    started_at = isoformat_utc(utc_now())
    db.update_batch_job(job_id, "running", started_at=started_at)
    processed_count = 0

    try:
        order_numbers = json.loads(job["order_numbers_json"])
        if not isinstance(order_numbers, list):
            raise RuntimeError("Batch job order list is invalid.")

        mode = str(job["mode"])
        dry_run = mode != "send"
        batch_size = int(job["batch_size"] or BATCH_CHUNK_SIZE)

        for start in range(0, len(order_numbers), batch_size):
            chunk = [str(value) for value in order_numbers[start : start + batch_size]]
            results = process_orders(sender, chunk, dry_run=dry_run)
            db.append_batch_results(job_id, results)
            processed_count += len(results)
            db.update_batch_job(job_id, "running", processed_count=processed_count)

        db.update_batch_job(
            job_id,
            "completed",
            processed_count=processed_count,
            completed_at=isoformat_utc(utc_now()),
        )
    except Exception as exc:
        db.update_batch_job(
            job_id,
            "failed",
            processed_count=processed_count,
            error_detail=str(exc),
            completed_at=isoformat_utc(utc_now()),
        )


def start_manual_batch_job(config: AppConfig, job_id: str) -> None:
    worker = threading.Thread(
        target=run_manual_batch_job,
        args=(config, job_id),
        daemon=True,
    )
    worker.start()


def require_admin_token(config: AppConfig) -> Optional[Response]:
    supplied = request.headers.get("X-Admin-Token") or request.args.get("token", "")
    if supplied != config.admin_token:
        return jsonify({"error": "Unauthorized"}), 401
    return None


def create_app(config: AppConfig) -> Flask:
    db = Database(config.database_path)
    sender = ReviewInviteSender(config)
    app = Flask(__name__)
    admin_state: Dict[str, Any] = {"last_run": None}

    @app.get("/health")
    def health() -> Response:
        return jsonify({"ok": True, "time": isoformat_utc(utc_now())})

    @app.post("/webhooks/shippo")
    def shippo_webhook() -> Response:
        if not config.shippo_webhook_token:
            return jsonify({"error": "Shippo webhook is not configured"}), 404
        supplied = request.args.get("token", "")
        if supplied != config.shippo_webhook_token:
            return jsonify({"error": "Unauthorized"}), 401

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "Expected JSON payload"}), 400

        tracking_details = extract_shippo_tracking_details(payload)
        if not tracking_details["tracking_number"]:
            return jsonify({"error": "Tracking number missing from payload"}), 400

        db.upsert_shipment(tracking_details)
        return jsonify({"ok": True})

    @app.post("/jobs/process")
    def process_job() -> Response:
        auth_error = require_admin_token(config)
        if auth_error:
            return auth_error

        dry_run = request.args.get("dry_run", "false").lower() == "true"
        results = process_due_reviews(config, db, sender, dry_run=dry_run)
        problems = db.list_problem_shipments(
            config.stale_tracking_hours,
            config.delivery_max_age_days,
        )
        return jsonify(
            {
                "processed": len(results),
                "results": results,
                "problem_orders": len(problems),
            }
        )

    @app.get("/reports/problem-orders")
    def problem_orders() -> Response:
        auth_error = require_admin_token(config)
        if auth_error:
            return auth_error

        rows = db.list_problem_shipments(
            config.stale_tracking_hours,
            config.delivery_max_age_days,
        )

        if request.args.get("format", "json").lower() != "csv":
            return jsonify({"count": len(rows), "rows": rows})

        fieldnames = [
            "order_number",
            "shopify_order_name",
            "customer_email",
            "tracking_number",
            "carrier",
            "last_tracking_status",
            "last_tracking_update_at",
            "delivered_at",
            "problem_reason",
        ]
        output_lines = [",".join(fieldnames)]
        for row in rows:
            output_lines.append(
                ",".join(
                    '"' + str(row[field]).replace('"', '""') + '"'
                    for field in fieldnames
                )
            )

        return Response(
            "\n".join(output_lines),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=problem_orders.csv"},
        )

    @app.get("/admin")
    def admin_page() -> Response:
        auth_error = require_admin_token(config)
        if auth_error:
            return auth_error

        rows = db.list_problem_shipments(
            config.stale_tracking_hours,
            config.delivery_max_age_days,
        )
        start_date = request.args.get("start_date", "")
        end_date = request.args.get("end_date", "")
        return Response(
            render_template_string(
                ADMIN_PAGE_TEMPLATE,
                token=config.admin_token,
                start_date=start_date,
                end_date=end_date,
                order_numbers_text="",
                message=request.args.get("message", ""),
                review_tag=config.review_tag,
                problem_rows=prepare_problem_rows_for_display(rows),
                last_run=admin_state["last_run"],
                **get_latest_batch_context(db),
            ),
            mimetype="text/html",
        )

    @app.post("/admin/upload-orders")
    def admin_upload_orders() -> Response:
        auth_error = require_admin_token(config)
        if auth_error:
            return auth_error

        mode = str(request.form.get("mode", "dry_run")).strip().lower()
        if mode not in {"dry_run", "send"}:
            return jsonify({"error": "Invalid mode"}), 400

        raw_text = str(request.form.get("order_numbers_text", "")).strip()
        uploaded_file = request.files.get("orders_file")
        if uploaded_file and uploaded_file.filename:
            payload = uploaded_file.read()
            try:
                uploaded_text = payload.decode("utf-8-sig")
            except UnicodeDecodeError:
                uploaded_text = payload.decode("utf-8", errors="ignore")
            if uploaded_text.strip():
                raw_text = f"{raw_text}\n{uploaded_text}".strip()

        rows = db.list_problem_shipments(
            config.stale_tracking_hours,
            config.delivery_max_age_days,
        )

        try:
            order_numbers = parse_order_numbers_text(raw_text)
            if not order_numbers:
                raise RuntimeError("Add at least one Shopify order number or upload a CSV file.")
            active_job = db.get_active_batch_job()
            if active_job:
                raise RuntimeError(
                    "A manual batch is already running. Wait for it to complete before starting another one."
                )

            job_id = db.create_batch_job(mode, order_numbers, BATCH_CHUNK_SIZE)
            start_manual_batch_job(config, job_id)
            message = (
                f"Manual batch started in {mode} mode. "
                f"{len(order_numbers)} order(s) will be processed in chunks of {BATCH_CHUNK_SIZE}. "
                "Refresh this page to see progress."
            )
            return Response(
                render_template_string(
                    ADMIN_PAGE_TEMPLATE,
                    token=config.admin_token,
                    start_date="",
                    end_date="",
                    order_numbers_text=raw_text,
                    message=message,
                    review_tag=config.review_tag,
                    problem_rows=prepare_problem_rows_for_display(rows),
                    last_run=admin_state["last_run"],
                    **get_latest_batch_context(db),
                ),
                mimetype="text/html",
            )
        except Exception as exc:
            return Response(
                render_template_string(
                    ADMIN_PAGE_TEMPLATE,
                    token=config.admin_token,
                    start_date="",
                    end_date="",
                    order_numbers_text=raw_text,
                    message=f"Upload failed: {exc}",
                    review_tag=config.review_tag,
                    problem_rows=prepare_problem_rows_for_display(rows),
                    last_run=admin_state["last_run"],
                    **get_latest_batch_context(db),
                ),
                mimetype="text/html",
                status=400,
            )

    @app.get("/admin/batch-job/<job_id>")
    def admin_batch_job(job_id: str) -> Response:
        auth_error = require_admin_token(config)
        if auth_error:
            return auth_error

        job = db.get_batch_job(job_id)
        if not job:
            return jsonify({"error": "Batch job not found"}), 404

        return jsonify(
            {
                "job": dict(job),
                "counts": db.list_batch_result_counts(job_id),
                "results": db.list_batch_results(job_id, limit=200),
            }
        )

    @app.post("/admin/backfill-date-range")
    def admin_backfill_date_range() -> Response:
        auth_error = require_admin_token(config)
        if auth_error:
            return auth_error

        start_date = str(request.form.get("start_date", "")).strip()
        end_date = str(request.form.get("end_date", "")).strip()
        mode = str(request.form.get("mode", "dry_run")).strip().lower()
        if mode not in {"dry_run", "send"}:
            return jsonify({"error": "Invalid mode"}), 400

        try:
            results = backfill_delivered_orders_from_shopify_date_range(
                sender,
                start_date,
                end_date,
                dry_run=(mode != "send"),
            )
            admin_state["last_run"] = build_last_run_payload(
                start_date,
                end_date,
                mode,
                results,
            )
            rows = db.list_problem_shipments(
                config.stale_tracking_hours,
                config.delivery_max_age_days,
            )
            message = (
                f"Date-range backfill completed in {mode} mode. "
                f"Checked {len(results)} order(s)."
            )
            return Response(
                render_template_string(
                    ADMIN_PAGE_TEMPLATE,
                    token=config.admin_token,
                    start_date=start_date,
                    end_date=end_date,
                    order_numbers_text="",
                    message=message,
                    review_tag=config.review_tag,
                    problem_rows=prepare_problem_rows_for_display(rows),
                    last_run=admin_state["last_run"],
                    **get_latest_batch_context(db),
                ),
                mimetype="text/html",
            )
        except Exception as exc:
            rows = db.list_problem_shipments(
                config.stale_tracking_hours,
                config.delivery_max_age_days,
            )
            return Response(
                render_template_string(
                    ADMIN_PAGE_TEMPLATE,
                    token=config.admin_token,
                    start_date=start_date,
                    end_date=end_date,
                    order_numbers_text="",
                    message=f"Backfill failed: {exc}",
                    review_tag=config.review_tag,
                    problem_rows=prepare_problem_rows_for_display(rows),
                    last_run=admin_state["last_run"],
                    **get_latest_batch_context(db),
                ),
                mimetype="text/html",
                status=400,
            )

    return app


def create_app_from_env() -> Flask:
    return create_app(load_config())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Review invite automation with Shippo webhook support."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the Flask web service.")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=None)

    due_parser = subparsers.add_parser(
        "process-pending",
        help="Send review invites for shipments delivered long enough ago.",
    )
    due_parser.add_argument("--dry-run", action="store_true")
    due_parser.add_argument("--send", action="store_true")
    due_parser.add_argument("--output", default="review_send_results.csv")

    problem_parser = subparsers.add_parser(
        "export-problems",
        help="Export stale or overdue shipments.",
    )
    problem_parser.add_argument("--output", default="problematic_orders.csv")

    csv_parser = subparsers.add_parser(
        "process-csv",
        help="Keep the old CSV-based review send flow for backfills.",
    )
    csv_parser.add_argument("--input", default="orders.csv")
    csv_parser.add_argument("--output", default="review_send_results.csv")
    csv_parser.add_argument("--dry-run", action="store_true")
    csv_parser.add_argument("--send", action="store_true")

    backfill_parser = subparsers.add_parser(
        "backfill-delivered",
        help="Send historical review emails only for orders Shippo confirms as delivered.",
    )
    backfill_parser.add_argument("--input", default="orders.csv")
    backfill_parser.add_argument("--output", default="review_backfill_results.csv")
    backfill_parser.add_argument("--dry-run", action="store_true")
    backfill_parser.add_argument("--send", action="store_true")

    date_backfill_parser = subparsers.add_parser(
        "backfill-date-range",
        help="Pull Shopify orders by date range, then send only for Shippo-confirmed deliveries.",
    )
    date_backfill_parser.add_argument("--start-date", required=True)
    date_backfill_parser.add_argument("--end-date", required=True)
    date_backfill_parser.add_argument("--output", default="review_date_backfill_results.csv")
    date_backfill_parser.add_argument("--dry-run", action="store_true")
    date_backfill_parser.add_argument("--send", action="store_true")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = load_config()
        db = Database(config.database_path)
        sender = ReviewInviteSender(config)

        if args.command == "serve":
            app = create_app(config)
            app.run(host=args.host, port=args.port or config.port)
            return 0

        if args.command == "process-pending":
            if args.dry_run and args.send:
                raise RuntimeError("Use either --dry-run or --send, not both.")
            if not args.dry_run and not args.send:
                raise RuntimeError("You must use either --dry-run or --send.")
            results = process_due_reviews(config, db, sender, dry_run=not args.send)
            write_results_csv(args.output, results)
            print_summary(results)
            print(f"\nDetailed results written to: {args.output}")
            return 0

        if args.command == "export-problems":
            rows = db.list_problem_shipments(
                config.stale_tracking_hours,
                config.delivery_max_age_days,
            )
            write_problem_csv(args.output, rows)
            print(f"Problem orders exported: {len(rows)}")
            print(f"Detailed results written to: {args.output}")
            return 0

        if args.command == "process-csv":
            if args.dry_run and args.send:
                raise RuntimeError("Use either --dry-run or --send, not both.")
            if not args.dry_run and not args.send:
                raise RuntimeError("You must use either --dry-run or --send.")
            order_numbers = read_order_numbers(args.input)
            results = process_orders(sender, order_numbers, dry_run=not args.send)
            write_results_csv(args.output, results)
            print_summary(results)
            print(f"\nDetailed results written to: {args.output}")
            return 0

        if args.command == "backfill-delivered":
            if args.dry_run and args.send:
                raise RuntimeError("Use either --dry-run or --send, not both.")
            if not args.dry_run and not args.send:
                raise RuntimeError("You must use either --dry-run or --send.")
            order_numbers = read_order_numbers(args.input)
            results = backfill_delivered_orders(sender, order_numbers, dry_run=not args.send)
            write_results_csv(args.output, results)
            print_summary(results)
            print(f"\nDetailed results written to: {args.output}")
            return 0

        if args.command == "backfill-date-range":
            if args.dry_run and args.send:
                raise RuntimeError("Use either --dry-run or --send, not both.")
            if not args.dry_run and not args.send:
                raise RuntimeError("You must use either --dry-run or --send.")
            results = backfill_delivered_orders_from_shopify_date_range(
                sender,
                args.start_date,
                args.end_date,
                dry_run=not args.send,
            )
            write_results_csv(args.output, results)
            print_summary(results)
            print(f"\nDetailed results written to: {args.output}")
            return 0

        raise RuntimeError(f"Unsupported command: {args.command}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
