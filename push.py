#!/usr/bin/env python3
"""
Upload .mp4 files from a folder to HIFFI server.
Each file is named {id}.mp4. Metadata comes from downloaded_videos.json.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

PASSWORD = "123456"
VIDEOS_JSON = "all.json"


def channel_to_username(channel: str) -> str:
    """Convert channel name to valid username: lowercase, spaces -> underscores."""
    if not channel:
        return "unknown"
    s = channel.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_") or "user"
    return s[:64]  # reasonable limit


def load_metadata() -> dict[str, dict]:
    """Load downloaded_videos.json as id -> metadata mapping."""
    with open(VIDEOS_JSON, "r") as f:
        records = json.load(f)
    return {r["id"]: r for r in records if r.get("id")}


def auth_login(base_url: str, username: str) -> str | None:
    """Try login. Returns token or None."""
    r = requests.post(
        f"{base_url}/auth/login",
        json={"username": username, "password": PASSWORD},
        timeout=30,
    )
    data = r.json()
    if data.get("success") and data.get("data", {}).get("token"):
        return data["data"]["token"]
    return None


def auth_register(base_url: str, username: str) -> str:
    """Register user and return token."""
    body = {
        "username": username,
        "name": username,
        "password": PASSWORD,
        "email": f"{username}@creators.hiffi.com",
    }
    r = requests.post(f"{base_url}/auth/register-direct", json=body, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("success") or not data.get("data", {}).get("token"):
        raise RuntimeError(f"Register failed: {data}")
    return data["data"]["token"]


def get_token(base_url: str, username: str) -> str:
    """Login or register, return token."""
    token = auth_login(base_url, username)
    if token:
        return token
    return auth_register(base_url, username)


def get_upload_links(
    base_url: str, token: str, video_title: str, video_description: str
) -> tuple[str, str, str]:
    """Step 2: Request upload URLs. Returns (bridge_id, gateway_url, gateway_url_thumbnail)."""
    r = requests.post(
        f"{base_url}/videos/upload",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "video_title": video_title,
            "video_description": video_description or "",
            "video_tags": [],
            "video_views": 0,
            "video_upvotes": 0,
            "video_downvotes": 0,
            "video_comments": 0,
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"Upload links failed: {data}")
    d = data["data"]
    return d["bridge_id"], d["gateway_url"], d["gateway_url_thumbnail"]


def extract_thumbnail(video_path: str, duration_sec: float, out_path: str) -> None:
    """Extract middle frame as thumbnail using ffmpeg."""
    mid = max(0, duration_sec / 2.0)
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss", str(mid),
            "-i", video_path,
            "-vframes", "1",
            "-q:v", "2",
            out_path,
        ],
        check=True,
        capture_output=True,
    )


def upload_to_presigned_url(url: str, file_path: str, content_type: str) -> None:
    """PUT file to presigned S3/R2 URL."""
    with open(file_path, "rb") as f:
        r = requests.put(
            url,
            data=f,
            headers={"Content-Type": content_type},
            timeout=600,
        )
    r.raise_for_status()


def acknowledge_upload(base_url: str, token: str, bridge_id: str) -> None:
    """Step 4: Acknowledge upload complete."""
    r = requests.post(
        f"{base_url}/videos/upload/ack/{bridge_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"Ack failed: {data}")


def upload_video(
    base_url: str,
    video_path: str,
    meta: dict,
    tokens_by_username: dict[str, str],
) -> None:
    """Upload a single video through the full flow."""
    channel = meta.get("channel") or "unknown"
    username = channel_to_username(channel)

    if username not in tokens_by_username:
        print(f"  Auth for {username}...")
        tokens_by_username[username] = get_token(base_url, username)

    token = tokens_by_username[username]

    title = meta.get("title") or os.path.basename(video_path)
    description = meta.get("description") or ""
    duration = float(meta.get("duration") or 0)

    print(f"  Step 2: Get upload links...")
    bridge_id, gateway_url, gateway_url_thumbnail = get_upload_links(
        base_url, token, title, description
    )

    print(f"  Step 3: Upload video + thumbnail...")
    upload_to_presigned_url(gateway_url, video_path, "video/mp4")

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
        thumb_path = tf.name
    try:
        extract_thumbnail(video_path, duration, thumb_path)
        upload_to_presigned_url(gateway_url_thumbnail, thumb_path, "image/jpeg")
    finally:
        os.unlink(thumb_path)

    print(f"  Step 4: Acknowledge...")
    acknowledge_upload(base_url, token, bridge_id)
    print(f"  Done: {bridge_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload .mp4 files to HIFFI server")
    parser.add_argument(
        "folder",
        nargs="?",
        default=".",
        help="Folder containing {id}.mp4 files",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("HIFFI_BASE_URL", "https://api.hiffi.com"),
        help="API base URL",
    )
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.is_dir():
        print(f"Error: {folder} is not a directory", file=sys.stderr)
        sys.exit(1)

    metadata = load_metadata()
    mp4_files = list(folder.glob("*.mp4"))

    if not mp4_files:
        print(f"No .mp4 files in {folder}")
        return

    missing = []
    to_upload = []
    for p in mp4_files:
        vid_id = p.stem
        if vid_id not in metadata:
            missing.append(vid_id)
            continue
        to_upload.append((p, metadata[vid_id]))

    if missing:
        print(f"Warning: no metadata for {len(missing)} files: {missing[:5]}{'...' if len(missing) > 5 else ''}")

    if not to_upload:
        print("No videos to upload (all missing metadata)")
        return

    print(f"Uploading {len(to_upload)} videos to {args.base_url}")
    tokens_by_username: dict[str, str] = {}
    done_dir = folder.parent / "downloads_done"
    done_dir.mkdir(parents=True, exist_ok=True)

    for video_path, meta in to_upload:
        vid_id = meta["id"]
        print(f"\n[{vid_id}] {meta.get('title', '')[:50]}...")
        try:
            upload_video(args.base_url, str(video_path), meta, tokens_by_username)
            shutil.move(str(video_path), str(done_dir / video_path.name))
        except Exception as e:
            print(f"  Failed: {e}", file=sys.stderr)
            raise

    print(f"\nUploaded {len(to_upload)} videos.")


if __name__ == "__main__":
    main()
    sys.exit(0)
