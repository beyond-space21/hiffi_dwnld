#!/usr/bin/env python3
"""
RabbitMQ consumer: take video URLs from the queue, download with yt_dlp, then ack.
Processes one message at a time. Updates a JSON file with video metadata after each download.
YouTube timeout → exit program. Rate limit → log and exit program. Other errors → log and skip.
"""

import json
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

import pika
from dotenv import load_dotenv

try:
    from yt_dlp import YoutubeDL
except ImportError:
    print("Install yt-dlp: pip install yt-dlp", file=sys.stderr)
    raise

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = SCRIPT_DIR / "downloads"
METADATA_JSON = SCRIPT_DIR / "downloaded_videos.json"
ERROR_LOG = SCRIPT_DIR / "download_errors.log"
COOKIES_FILE = SCRIPT_DIR / "cookies.txt"
DELAY_BETWEEN_DOWNLOADS_SEC = 5

RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/")
RABBITMQ_QUEUE = os.environ["RABBITMQ_QUEUE"]


def ensure_downloads_dir() -> None:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)


def load_metadata() -> list:
    if not METADATA_JSON.exists():
        return []
    try:
        with open(METADATA_JSON, "r", encoding="utf-8") as f:
            data = f.read().strip()
            if not data:
                return []
            return json.loads(data)
    except (json.JSONDecodeError, OSError):
        return []


def save_metadata(records: list) -> None:
    with open(METADATA_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def video_id_from_url(url: str) -> str | None:
    """Extract YouTube video ID from url (youtube.com/watch?v=ID)."""
    if "watch?v=" in url:
        return url.split("watch?v=", 1)[1].split("&")[0].strip()
    if "youtu.be/" in url:
        return url.split("youtu.be/", 1)[1].split("?")[0].strip()
    return None


def is_timeout_error(exc: BaseException) -> bool:
    """True if the exception is a timeout (YouTube/timeout → exit program)."""
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return True
    msg = (getattr(exc, "msg", None) or str(exc)).lower()
    if "timeout" in msg or "timed out" in msg:
        return True
    cause = getattr(exc, "__cause__", None)
    if cause and is_timeout_error(cause):
        return True
    return False


def is_rate_limit_error(exc: BaseException) -> bool:
    """True if the exception indicates rate limiting (→ log and exit program)."""
    msg = (getattr(exc, "msg", None) or str(exc)).lower()
    if "429" in msg or "rate limit" in msg or "too many requests" in msg:
        return True
    cause = getattr(exc, "__cause__", None)
    if cause and is_rate_limit_error(cause):
        return True
    return False


def log_error(video_id: str, error: BaseException) -> None:
    """Append a single line to the error log: simple detail + video id."""
    detail = (getattr(error, "msg", None) or str(error)).strip().replace("\n", " ")
    if len(detail) > 200:
        detail = detail[:197] + "..."
    line = f"{datetime.now().isoformat()} video_id={video_id} {detail}\n"
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(line)


def download_and_collect_metadata(url: str) -> dict | None:
    """Download video to DOWNLOADS_DIR and return metadata dict for JSON."""
    ensure_downloads_dir()
    outtmpl = str(DOWNLOADS_DIR / "%(id)s.%(ext)s")

    ydl_opts = {
        "outtmpl": outtmpl,
        "quiet": False,
        "merge_output_format": "mp4",
        "format": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=720]+bestaudio/best[height<=720]/best",
    }

    # Use cookies from cookies.txt if present (Netscape format for yt-dlp).
    if COOKIES_FILE.exists():
        ydl_opts["cookiefile"] = str(COOKIES_FILE)

    # Enable remote EJS components so yt-dlp can solve YouTube JS challenges
    # when using an external JS runtime (deno in this environment).
    # Equivalent to CLI: --remote-components ejs:github
    ydl_opts["remote_components"] = ["ejs:github"]

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        if not info:
            return None

        vid = info.get("id") or video_id_from_url(url)
        channel = info.get("channel") or info.get("uploader") or ""
        duration = info.get("duration")
        title = info.get("title") or ""
        description = (info.get("description") or "").strip()

        return {
            "id": vid,
            "channel": channel,
            "duration": duration,
            "title": title,
            "description": description,
        }


def run_consumer() -> None:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
    )

    conn = pika.BlockingConnection(parameters)
    channel = conn.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    timeout_occurred: list[bool] = [False]
    rate_limit_occurred: list[bool] = [False]

    def on_message(ch, method, properties, body):
        url = body.decode("utf-8").strip()
        video_id = video_id_from_url(url) or "unknown"
        if not url:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"Processing: {url}")
        try:
            meta = download_and_collect_metadata(url)
            if meta:
                records = load_metadata()
                records.append(meta)
                save_metadata(records)
                title_preview = (meta.get("title") or "")[:50]
                if len(meta.get("title") or "") > 50:
                    title_preview += "..."
                print(f"  Saved metadata: id={meta.get('id')}, title={title_preview}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            time.sleep(DELAY_BETWEEN_DOWNLOADS_SEC)
        except Exception as e:
            if is_timeout_error(e):
                print(f"  Timeout: {e}", file=sys.stderr)
                timeout_occurred[0] = True
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                ch.stop_consuming()
                return
            if is_rate_limit_error(e):
                log_error(video_id, e)
                print(f"  Rate limited: {e}", file=sys.stderr)
                rate_limit_occurred[0] = True
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                ch.stop_consuming()
                return
            log_error(video_id, e)
            print(f"  Skipped (logged): {e}", file=sys.stderr)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            time.sleep(DELAY_BETWEEN_DOWNLOADS_SEC)

    channel.basic_consume(
        queue=RABBITMQ_QUEUE,
        on_message_callback=on_message,
        auto_ack=False,
    )

    print(f"Consuming from queue '{RABBITMQ_QUEUE}'. Downloads: {DOWNLOADS_DIR}")
    print("Errors logged to:", ERROR_LOG)
    print("Ctrl+C to stop.")
    channel.start_consuming()

    if timeout_occurred[0]:
        print("Exiting due to YouTube timeout.", file=sys.stderr)
        sys.exit(1)
    if rate_limit_occurred[0]:
        print("Exiting due to rate limit.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run_consumer()
    sys.exit(0)
