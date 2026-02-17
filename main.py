import json
import sys
from pathlib import Path
from typing import Iterable, List, Optional

try:
    from yt_dlp import YoutubeDL
except ImportError:
    print(
        "The 'yt-dlp' package is required.\n"
        "Install it with:\n\n"
        "    pip install yt-dlp\n",
        file=sys.stderr,
    )
    raise

CHANNELS_FILE = Path("channels.det")
OUTPUT_JSON = Path("videos.json")


def normalize_channel_url(url: str) -> str:
    """Append /videos to YouTube channel URLs so we get the full upload list."""
    url = url.strip().rstrip("/")
    if "youtube.com" not in url or "/videos" in url:
        return url
    return f"{url}/videos"


def iter_channel_urls(path: Path) -> Iterable[str]:
    """Yield non-empty, non-comment lines as channel URLs."""
    if not path.exists():
        raise FileNotFoundError(f"Channels file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            yield line


def scrape_channel_videos(ydl: YoutubeDL, channel_url: str) -> List[dict]:
    """Use yt-dlp to get all video links with duration and channel."""
    print(f"Scraping channel: {channel_url}")

    info = ydl.extract_info(channel_url, download=False)
    entries = info.get("entries") or []
    channel_name = info.get("channel") or info.get("uploader") or info.get("title") or None

    videos: List[dict] = []
    for entry in entries:
        if entry is None:
            continue

        video_id = entry.get("id")
        video_url = entry.get("url") or entry.get("webpage_url") or (
            f"https://www.youtube.com/watch?v={video_id}" if video_id else None
        )
        if not video_url:
            continue

        duration = entry.get("duration")
        channel = entry.get("channel") or channel_name

        videos.append({
            "url": video_url,
            "duration": duration,
            "channel": channel,
        })

    print(f"  Found {len(videos)} videos")
    return videos


def main() -> None:
    if not CHANNELS_FILE.exists():
        print(f"Channels file not found: {CHANNELS_FILE}", file=sys.stderr)
        sys.exit(1)

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
        "extract_flat": False,
    }

    all_videos: List[dict] = []
    with YoutubeDL(ydl_opts) as ydl:
        for url in iter_channel_urls(CHANNELS_FILE):
            try:
                channel_videos = scrape_channel_videos(ydl, normalize_channel_url(url))
                all_videos.extend(channel_videos)
            except Exception as e:
                print(f"Failed to scrape {url}: {e}", file=sys.stderr)

    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(all_videos, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(all_videos)} videos to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
