"""
conan_automation_github.py - Detective Conan downloader/uploader for GitHub Actions.

Main features:
- separate subtitle magnets
- external subtitle matching by episode, movie, special, and OVA number
- embedded English subtitle auto-selection via ffprobe
- Nyaa search using multiple strategies, sorted by best match and most seeds
- automatic source failover when a torrent stays stuck near 0 B or is too slow
- automatic chunking for very large multi-file torrents
- sequential download -> upload -> patch -> commit -> delete processing
- DoodStream auto naming with sortable titles
- optional bulk Dood sync through update.py
"""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from conan_utils import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    ZIP_EXTENSIONS,
    compress_select_spec,
    format_dood_title,
    parse_file_info,
    parse_select_spec,
    read_torrent_manifest,
)
from update import (
    bulk_sync,
    patch_hs,
    patch_movie_hs,
    patch_movie_ss,
    patch_ova_url,
    patch_special_url,
    patch_ss,
    read_html,
    write_html,
)

# Config
DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "")
HARD_SUB_FOLDER_ID = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID = os.environ.get("SOFT_SUB_FOLDER_ID", "")

BASE_EPISODE = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE = os.environ.get("BASE_DATE", "2026-03-14")

EPISODE_OVERRIDE = os.environ.get("EPISODE_OVERRIDE", "").strip()
MAGNET_LINKS = os.environ.get("MAGNET_LINKS", "").strip()
SUBTITLE_MAGNET_LINKS = os.environ.get("SUBTITLE_MAGNET_LINKS", "").strip()
SELECT_FILES = os.environ.get("SELECT_FILES", "").strip()
SUBTITLE_SELECT_FILES = os.environ.get("SUBTITLE_SELECT_FILES", "").strip() or SELECT_FILES
CUSTOM_SEARCH = os.environ.get("CUSTOM_SEARCH", "").strip()
NYAA_UPLOADER_URL = os.environ.get("NYAA_UPLOADER_URL", "").strip()

MOVIE_MODE = os.environ.get("MOVIE_MODE", "0").strip() == "1"

HTML_FILE = os.environ.get("HTML_FILE", "index.html")
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", "downloads")
TEMP_ROOT = os.environ.get("TEMP_ROOT", os.path.join(DOWNLOAD_ROOT, ".tmp"))

ANIME_SEARCH_QUERY = os.environ.get("ANIME_SEARCH_QUERY", "").strip()
ANIME_BATCH_LIMIT_GB = float(os.environ.get("ANIME_BATCH_LIMIT_GB", "100") or "100")
ANIME_BATCH_INDEX = max(1, int(os.environ.get("ANIME_BATCH_INDEX", "1") or "1"))
ANIME_MAX_PAGES = max(1, int(os.environ.get("ANIME_MAX_PAGES", "10") or "10"))

MIN_FREE_GB = float(os.environ.get("MIN_FREE_GB", "8") or "8")
STALL_ZERO_SECONDS = int(os.environ.get("STALL_ZERO_SECONDS", "180") or "180")
LOW_SPEED_SECONDS = int(os.environ.get("LOW_SPEED_SECONDS", "180") or "180")
MIN_SPEED_BYTES = int(float(os.environ.get("MIN_SPEED_MB", "1") or "1") * 1024 * 1024)
MONITOR_INTERVAL = int(os.environ.get("MONITOR_INTERVAL", "10") or "10")
HUGE_TORRENT_THRESHOLD_GB = float(os.environ.get("HUGE_TORRENT_THRESHOLD_GB", "350") or "350")
HUGE_TORRENT_GROUPS = max(2, int(os.environ.get("HUGE_TORRENT_GROUPS", "5") or "5"))

UPLOAD_RETRIES = 3
RETRY_DELAY = 10
ARIA2_TIMEOUT = int(os.environ.get("ARIA2_TIMEOUT", "14400") or "14400")
ENGLISH_TAGS = {"eng", "en", "english"}
SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "kib": 1024,
    "mb": 1000 ** 2,
    "mib": 1024 ** 2,
    "gb": 1000 ** 3,
    "gib": 1024 ** 3,
    "tb": 1000 ** 4,
    "tib": 1024 ** 4,
}

_upload_server_url: str | None = None


@dataclass
class NyaaResult:
    title: str
    magnet: str
    seeds: int
    size_bytes: int
    source_url: str
    strategy_label: str
    score: int


@dataclass
class ProcessResult:
    number: int
    content_kind: str
    hs_url: str | None
    ss_url: str | None


# Generic helpers


def ensure_dirs() -> None:
    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
    os.makedirs(TEMP_ROOT, exist_ok=True)



def get_free_gb(path: str = DOWNLOAD_ROOT) -> float:
    usage = shutil.disk_usage(path)
    return usage.free / (1024 ** 3)



def storage_limit_hit() -> bool:
    free_gb = get_free_gb()
    if free_gb < MIN_FREE_GB:
        print(f"  Free space {free_gb:.2f} GiB is below limit {MIN_FREE_GB:.2f} GiB", file=sys.stderr)
        return True
    return False



def cleanup_path(path: str) -> None:
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.exists(path):
            os.remove(path)
    except OSError:
        pass



def cleanup_paths(paths: list[str]) -> None:
    for path in paths:
        cleanup_path(path)



def cleanup_empty_dirs(root: str = DOWNLOAD_ROOT) -> None:
    root_path = Path(root)
    if not root_path.exists():
        return
    for path in sorted(root_path.rglob("*"), reverse=True):
        if path.is_dir():
            try:
                path.rmdir()
            except OSError:
                pass



def make_work_dir(prefix: str) -> str:
    ensure_dirs()
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", prefix).strip("_") or "job"
    work_dir = os.path.join(TEMP_ROOT, f"{safe}_{int(time.time() * 1000)}")
    os.makedirs(work_dir, exist_ok=True)
    return work_dir



def parse_episode_override(raw: str) -> list[int]:
    raw = raw.strip()
    if not raw:
        return [get_auto_episode()]
    episodes: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            try:
                start = int(left.strip())
                end = int(right.strip())
            except ValueError:
                print(f"  WARNING: could not parse range '{part}' - skipping", file=sys.stderr)
                continue
            if start > end:
                start, end = end, start
            episodes.extend(range(start, end + 1))
        else:
            try:
                episodes.append(int(part))
            except ValueError:
                print(f"  WARNING: could not parse episode '{part}' - skipping", file=sys.stderr)
    if not episodes:
        return [get_auto_episode()]
    unique: list[int] = []
    seen: set[int] = set()
    for episode in episodes:
        if episode not in seen:
            seen.add(episode)
            unique.append(episode)
    return unique



def get_auto_episode() -> int:
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    weeks = max(0, (datetime.now() - base_dt).days // 7)
    return BASE_EPISODE + weeks



def parse_magnet_list(raw: str) -> list[str]:
    normalized = raw.replace(",magnet:", "\nmagnet:")
    out = []
    for line in normalized.splitlines():
        line = line.strip()
        if line.startswith("magnet:"):
            out.append(line)
    return out



def validate_select_files(raw: str) -> str:
    indexes = parse_select_spec(raw)
    return compress_select_spec(indexes)


# Nyaa helpers


def _build_nyaa_urls(number: int, content_kind: str = "episode") -> list[tuple[str, str]]:
    queries: list[str] = []
    if CUSTOM_SEARCH:
        queries.append(CUSTOM_SEARCH)

    if content_kind == "movie":
        queries.extend(
            [
                f"Detective Conan Movie {number} 1080p",
                f"Meitantei Conan Movie {number} 1080p",
                f"Detective Conan Movie {number}",
                f"Case Closed Movie {number}",
                f"Detective Conan Film {number}",
                f"Conan Movie {number}",
            ]
        )
    elif content_kind == "special":
        queries.extend(
            [
                f"Detective Conan Special {number} 1080p",
                f"Meitantei Conan Special {number} 1080p",
                f"Detective Conan Special {number}",
                f"Case Closed Special {number}",
                f"Detective Conan TV Special {number}",
                f"Conan Special {number}",
            ]
        )
    elif content_kind == "ova":
        queries.extend(
            [
                f"Detective Conan OVA {number} 1080p",
                f"Meitantei Conan OVA {number} 1080p",
                f"Detective Conan OVA {number}",
                f"Case Closed OVA {number}",
                f"Detective Conan Magic File {number}",
                f"Conan OVA {number}",
            ]
        )
    else:
        queries.extend(
            [
                f"Detective Conan - {number} 1080p",
                f"Detective Conan {number} 1080p",
                f"Meitantei Conan {number} 1080p",
                f"Case Closed {number} 1080p",
                f"Detective Conan - {number}",
                f"Detective Conan {number}",
            ]
        )

    uploader_base = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else ""
    seen: set[tuple[str, str]] = set()
    urls: list[tuple[str, str]] = []
    for index, query in enumerate(queries[:6], start=1):
        encoded = requests.utils.quote(query)
        for category, suffix in (("1_2", "anime"), ("0_0", "full-site")):
            if uploader_base:
                url = f"{uploader_base}?f=0&c={category}&q={encoded}&s=seeders&o=desc"
            else:
                url = f"https://nyaa.si/?f=0&c={category}&q={encoded}&s=seeders&o=desc"
            label = f"strategy {index} {suffix}"
            key = (label, url)
            if key not in seen:
                seen.add(key)
                urls.append(key)
    return urls



def _extract_seeders(cells: list[str]) -> int:
    numeric: list[int] = []
    for cell in cells:
        text = cell.replace(",", "").strip()
        if re.fullmatch(r"\d+", text):
            numeric.append(int(text))
    if len(numeric) >= 3:
        return numeric[-3]
    if numeric:
        return numeric[-1]
    return 0



def _parse_size_bytes(text: str) -> int:
    match = re.search(r"(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)", text, re.IGNORECASE)
    if not match:
        return 0
    return int(float(match.group(1)) * SIZE_UNITS[match.group(2).lower()])



def _extract_size_bytes(cells: list[str]) -> int:
    for cell in cells:
        if re.search(r"\b(?:KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)\b", cell, re.IGNORECASE):
            return _parse_size_bytes(cell)
    return 0



def _score_nyaa_result(title: str, number: int, content_kind: str, seeds: int) -> int:
    lower = title.lower()
    score = 0
    if re.search(rf"(?<!\d){number}(?!\d)", title):
        score += 1000
    if "detective conan" in lower or "meitantei conan" in lower or "case closed" in lower:
        score += 200
    if "1080p" in lower:
        score += 100
    if "720p" in lower:
        score += 30
    if "subsplease" in lower or "erai" in lower or "ember" in lower:
        score += 40
    if content_kind == "movie" and "movie" in lower:
        score += 90
    if content_kind == "special" and "special" in lower:
        score += 90
    if content_kind == "ova" and ("ova" in lower or "magic file" in lower or "bonus file" in lower):
        score += 90
    if content_kind == "episode" and any(bad in lower for bad in ("movie", "special", "ova", "magic file", "bonus file")):
        score -= 250
    if "batch" in lower and content_kind == "episode":
        score -= 50
    score += min(seeds, 1000)
    return score



def search_nyaa_candidates(number: int, content_kind: str = "episode") -> list[NyaaResult]:
    results: dict[str, NyaaResult] = {}
    for label, url in _build_nyaa_urls(number, content_kind):
        print(f"  Searching Nyaa ({label}): {url}")
        try:
            response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
        except Exception as exc:
            print(f"  Nyaa error ({label}): {exc}", file=sys.stderr)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.success, tr.default, tr.danger")
        for row in rows:
            title_tag = row.select_one("a[title]")
            title = (title_tag.get("title") if title_tag else "") or (title_tag.get_text(" ", strip=True) if title_tag else "")
            if not title:
                continue
            magnet = None
            for link in row.find_all("a", href=True):
                href = link.get("href", "")
                if href.startswith("magnet:"):
                    magnet = href
                    break
            if not magnet:
                continue
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            seeds = _extract_seeders(cells)
            size_bytes = _extract_size_bytes(cells)
            score = _score_nyaa_result(title, number, content_kind, seeds)
            existing = results.get(magnet)
            candidate = NyaaResult(title=title, magnet=magnet, seeds=seeds, size_bytes=size_bytes, source_url=url, strategy_label=label, score=score)
            if existing is None or (candidate.score, candidate.seeds) > (existing.score, existing.seeds):
                results[magnet] = candidate

    ordered = sorted(results.values(), key=lambda item: (item.score, item.seeds, item.size_bytes), reverse=True)
    if ordered:
        print(f"  Found {len(ordered)} source candidate(s). Best: {ordered[0].title} | seeds={ordered[0].seeds}")
    return ordered



def search_nyaa_all(query: str, max_pages: int = ANIME_MAX_PAGES) -> list[NyaaResult]:
    if not query.strip():
        return []
    out: dict[str, NyaaResult] = {}
    encoded = requests.utils.quote(query)
    for page in range(1, max_pages + 1):
        url = f"https://nyaa.si/?f=0&c=1_2&q={encoded}&s=seeders&o=desc&p={page}"
        print(f"  Bulk Nyaa search page {page}: {url}")
        try:
            response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
        except Exception as exc:
            print(f"  Nyaa bulk search error page {page}: {exc}", file=sys.stderr)
            break
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.success, tr.default, tr.danger")
        if not rows:
            break
        added = 0
        for row in rows:
            title_tag = row.select_one("a[title]")
            title = (title_tag.get("title") if title_tag else "") or (title_tag.get_text(" ", strip=True) if title_tag else "")
            if not title:
                continue
            magnet = None
            for link in row.find_all("a", href=True):
                href = link.get("href", "")
                if href.startswith("magnet:"):
                    magnet = href
                    break
            if not magnet:
                continue
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            seeds = _extract_seeders(cells)
            size_bytes = _extract_size_bytes(cells)
            out.setdefault(
                magnet,
                NyaaResult(title=title, magnet=magnet, seeds=seeds, size_bytes=size_bytes, source_url=url, strategy_label="bulk", score=seeds),
            )
            added += 1
        if added == 0:
            break
    ordered = sorted(out.values(), key=lambda item: (item.seeds, item.size_bytes, item.title.lower()), reverse=True)
    print(f"  Bulk search found {len(ordered)} result(s)")
    return ordered



def build_size_batches(results: list[NyaaResult], limit_bytes: int) -> list[list[NyaaResult]]:
    if not results:
        return []
    if limit_bytes <= 0:
        return [results[:]]
    batches: list[list[NyaaResult]] = []
    current: list[NyaaResult] = []
    current_bytes = 0
    for item in results:
        item_size = max(1, item.size_bytes)
        if current and current_bytes + item_size > limit_bytes:
            batches.append(current)
            current = []
            current_bytes = 0
        current.append(item)
        current_bytes += item_size
        if item_size >= limit_bytes:
            batches.append(current)
            current = []
            current_bytes = 0
    if current:
        batches.append(current)
    return batches


# Torrent metadata + chunking


def fetch_torrent_metadata(magnet: str) -> str | None:
    work_dir = make_work_dir("meta")
    cmd = [
        "aria2c",
        f"--dir={work_dir}",
        "--seed-time=0",
        "--bt-metadata-only=true",
        "--bt-save-metadata=true",
        "--follow-torrent=mem",
        magnet,
    ]
    try:
        subprocess.run(cmd, check=True, timeout=900, capture_output=True, text=True)
    except Exception as exc:
        print(f"  Could not fetch torrent metadata: {exc}", file=sys.stderr)
    for path in glob.glob(os.path.join(work_dir, "**", "*.torrent"), recursive=True):
        return path
    cleanup_path(work_dir)
    return None



def build_select_groups_from_manifest(
    manifest: dict[str, Any] | None,
    explicit_select: str,
    wanted_extensions: set[str],
) -> list[str]:
    explicit = validate_select_files(explicit_select)
    if explicit:
        return [explicit]
    if not manifest:
        return [""]

    files = manifest.get("files") or []
    filtered = []
    for entry in files:
        ext = os.path.splitext(str(entry.get("path") or ""))[1].lower()
        if ext in wanted_extensions:
            filtered.append(entry)
    if not filtered:
        filtered = files
    if not filtered:
        return [""]

    total_size = sum(int(entry.get("length", 0) or 0) for entry in filtered)
    total_gb = total_size / (1024 ** 3)
    indexes = [int(entry["index"]) for entry in filtered]
    if total_gb <= HUGE_TORRENT_THRESHOLD_GB or len(indexes) < HUGE_TORRENT_GROUPS:
        return [compress_select_spec(indexes)]

    target = max(1, total_size // HUGE_TORRENT_GROUPS)
    groups: list[list[int]] = []
    current_indexes: list[int] = []
    current_size = 0
    remaining_groups = HUGE_TORRENT_GROUPS
    remaining_files = len(filtered)
    for entry in filtered:
        idx = int(entry["index"])
        size = int(entry.get("length", 0) or 0)
        current_indexes.append(idx)
        current_size += size
        remaining_files -= 1
        must_cut = current_size >= target and remaining_groups > 1 and remaining_files >= remaining_groups - 1
        if must_cut:
            groups.append(current_indexes)
            current_indexes = []
            current_size = 0
            remaining_groups -= 1
    if current_indexes:
        groups.append(current_indexes)

    specs = [compress_select_spec(group) for group in groups if group]
    if specs:
        print(f"  Large multi-file torrent detected ({total_gb:.2f} GiB). Split into {len(specs)} section(s).")
        return specs
    return [compress_select_spec(indexes)]


# Download helpers


def _snapshot_files(root: str, extensions: set[str] | None = None) -> list[str]:
    found: list[str] = []
    for path in glob.glob(os.path.join(root, "**", "*"), recursive=True):
        if not os.path.isfile(path):
            continue
        if extensions is not None and os.path.splitext(path)[1].lower() not in extensions:
            continue
        found.append(os.path.normpath(path))
    return sorted(found)



def _dir_size_bytes(root: str) -> int:
    total = 0
    for path in glob.glob(os.path.join(root, "**", "*"), recursive=True):
        if os.path.isfile(path):
            try:
                total += os.path.getsize(path)
            except OSError:
                pass
    return total



def _tail_text(path: str, max_chars: int = 1200) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            data = fh.read()
        return data[-max_chars:]
    except Exception:
        return ""



def run_monitored_aria2(magnet: str, work_dir: str, select_spec: str) -> tuple[bool, str]:
    log_path = os.path.join(work_dir, "aria2.log")
    cmd = [
        "aria2c",
        f"--dir={work_dir}",
        "--seed-time=0",
        "--max-connection-per-server=8",
        "--split=8",
        "--file-allocation=none",
        "--bt-stop-timeout=300",
        "--enable-dht=true",
        "--enable-peer-exchange=true",
        "--bt-enable-lpd=true",
        "--continue=true",
        "--follow-torrent=true",
        "--summary-interval=5",
        magnet,
    ]
    if select_spec:
        cmd.insert(-1, f"--select-file={select_spec}")
        print(f"  Using SELECT_FILES={select_spec}")

    print(f"  aria2c work dir: {work_dir}")
    with open(log_path, "w", encoding="utf-8") as log_file:
        process = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, text=True)

    start = time.time()
    last_check = start
    last_size = 0
    low_speed_for = 0.0

    while True:
        ret = process.poll()
        now = time.time()
        size_bytes = _dir_size_bytes(work_dir)
        elapsed = now - start
        delta_t = max(1.0, now - last_check)
        speed = max(0, size_bytes - last_size) / delta_t

        if elapsed >= STALL_ZERO_SECONDS and size_bytes < 1024 * 1024:
            process.kill()
            process.wait(timeout=30)
            return False, "stuck near 0 B for too long"

        if elapsed >= STALL_ZERO_SECONDS and size_bytes >= 1024 * 1024:
            if speed < MIN_SPEED_BYTES:
                low_speed_for += delta_t
            else:
                low_speed_for = 0.0
            if low_speed_for >= LOW_SPEED_SECONDS:
                process.kill()
                process.wait(timeout=30)
                return False, f"speed stayed below {MIN_SPEED_BYTES / (1024 * 1024):.2f} MiB/s too long"

        if storage_limit_hit():
            process.kill()
            process.wait(timeout=30)
            return False, "storage limit reached"

        if ret is not None:
            if ret == 0:
                return True, "ok"
            return False, f"aria2c exited with code {ret}"

        time.sleep(MONITOR_INTERVAL)
        last_check = now
        last_size = size_bytes



def _extract_zip_subtitles(paths: list[str]) -> list[str]:
    extracted: list[str] = []
    for archive in paths:
        if os.path.splitext(archive)[1].lower() not in ZIP_EXTENSIONS:
            continue
        out_dir = os.path.splitext(archive)[0] + "_unzipped"
        os.makedirs(out_dir, exist_ok=True)
        try:
            subprocess.run(["unzip", "-o", archive, "-d", out_dir], check=True, capture_output=True, text=True, timeout=600)
        except Exception as exc:
            print(f"  Could not unzip subtitle archive '{archive}': {exc}", file=sys.stderr)
            continue
        extracted.extend(_snapshot_files(out_dir, SUBTITLE_EXTENSIONS))
    return extracted



def download_magnet_once(magnet: str, select_spec: str, wanted_extensions: set[str], work_dir: str) -> tuple[list[str], str]:
    ok, reason = run_monitored_aria2(magnet, work_dir, select_spec)
    if not ok:
        tail = _tail_text(os.path.join(work_dir, "aria2.log"))
        if tail:
            print(tail, file=sys.stderr)
        return [], reason

    files = _snapshot_files(work_dir, wanted_extensions | ZIP_EXTENSIONS)
    if wanted_extensions == SUBTITLE_EXTENSIONS:
        files.extend(_extract_zip_subtitles([path for path in files if os.path.splitext(path)[1].lower() in ZIP_EXTENSIONS]))
        files = _snapshot_files(work_dir, SUBTITLE_EXTENSIONS) + [path for path in files if os.path.splitext(path)[1].lower() in SUBTITLE_EXTENSIONS]
    filtered = []
    seen: set[str] = set()
    for path in files:
        ext = os.path.splitext(path)[1].lower()
        if ext not in wanted_extensions:
            continue
        norm = os.path.normpath(path)
        if norm not in seen:
            seen.add(norm)
            filtered.append(norm)
    return filtered, "ok"


# Subtitle helpers


def _subtitle_score(path: str, number: int, content_kind: str) -> tuple[int, int]:
    base = os.path.basename(path).lower()
    score = 0
    parsed_number, parsed_kind = parse_file_info(path, force_movie=MOVIE_MODE)
    if parsed_number == number:
        score += 100
    if parsed_kind == content_kind:
        score += 25
    if any(tag in base for tag in ("english", " eng ", ".eng.", "_eng", "[eng]", " en ")):
        score += 40
    ext = os.path.splitext(path)[1].lower()
    if ext == ".ass":
        score += 10
    elif ext == ".ssa":
        score += 8
    elif ext == ".srt":
        score += 6
    try:
        mtime = int(os.path.getmtime(path))
    except OSError:
        mtime = 0
    return score, mtime



def find_matching_external_subtitle(video_file: str, subtitle_files: list[str]) -> str | None:
    number, content_kind = parse_file_info(video_file, force_movie=MOVIE_MODE)
    if number is None:
        return None
    candidates = []
    for subtitle in subtitle_files:
        sub_num, sub_kind = parse_file_info(subtitle, force_movie=MOVIE_MODE)
        if sub_num == number and (sub_kind == content_kind or content_kind in ("episode", sub_kind)):
            candidates.append(subtitle)
    if not candidates:
        return None
    candidates.sort(key=lambda item: _subtitle_score(item, number, content_kind), reverse=True)
    return candidates[0]



def get_embedded_english_subtitle_index(input_file: str) -> int | None:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "s",
        "-show_entries",
        "stream=index:stream_tags=language,title",
        "-of",
        "json",
        input_file,
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
    except Exception as exc:
        print(f"  ffprobe subtitle scan failed: {exc}", file=sys.stderr)
        return None
    try:
        data = json.loads(result.stdout)
    except Exception:
        return None
    streams = data.get("streams") or []
    if not streams:
        return None
    best_index = None
    best_score = -1
    for subtitle_pos, stream in enumerate(streams):
        tags = stream.get("tags") or {}
        language = str(tags.get("language") or "").strip().lower()
        title = str(tags.get("title") or "").strip().lower()
        score = 0
        if language in ENGLISH_TAGS:
            score += 100
        if "english" in title or "eng" in title:
            score += 50
        if score > best_score:
            best_score = score
            best_index = subtitle_pos
    return 0 if best_index is None else best_index


# ffmpeg helpers


def _esc(path: str) -> str:
    value = path.replace("\\", "\\\\").replace("'", "\\'")
    return value.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")



def _mp4_ok(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 10 * 1024 * 1024



def remux_to_mp4(input_file: str, output_dir: str, label: str) -> str | None:
    output = os.path.join(output_dir, f"{label}_ss.mp4")
    cleanup_path(output)
    attempts = [
        ("stream copy", ["-c:v", "copy", "-c:a", "copy"]),
        ("video copy + audio aac", ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]),
        ("re-encode", ["-c:v", "libx264", "-preset", "veryfast", "-crf", "22", "-c:a", "aac", "-b:a", "192k"]),
    ]
    for desc, flags in attempts:
        cleanup_path(output)
        cmd = ["ffmpeg", "-y", "-i", input_file, *flags, "-sn", "-movflags", "+faststart", output]
        print(f"  Remux attempt ({desc})...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=ARIA2_TIMEOUT)
        if result.returncode == 0 and _mp4_ok(output):
            print(f"  Remux OK: {output}")
            return output
        if result.stderr:
            print(result.stderr[-500:], file=sys.stderr)
    return None



def hardsub(input_file: str, output_dir: str, label: str, external_subtitle: str | None = None) -> str | None:
    output = os.path.join(output_dir, f"{label}_hs.mp4")
    cleanup_path(output)
    if external_subtitle:
        filters = [f"subtitles='{_esc(external_subtitle)}'", f"subtitles={_esc(external_subtitle)}"]
    else:
        subtitle_index = get_embedded_english_subtitle_index(input_file)
        if subtitle_index is None:
            filters = [f"subtitles='{_esc(input_file)}'", f"subtitles={_esc(input_file)}"]
        else:
            filters = [f"subtitles='{_esc(input_file)}':si={subtitle_index}", f"subtitles={_esc(input_file)}:si={subtitle_index}"]
    for vf in filters:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_file,
            "-vf",
            vf,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "22",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            output,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=ARIA2_TIMEOUT)
            if _mp4_ok(output):
                print(f"  Hard-sub OK: {output}")
                return output
        except subprocess.CalledProcessError as exc:
            if exc.stderr:
                print(exc.stderr[-500:], file=sys.stderr)
    return None


# Dood upload helpers


def get_upload_server() -> str | None:
    global _upload_server_url
    if _upload_server_url:
        return _upload_server_url
    try:
        resp = requests.get(
            "https://doodapi.co/api/upload/server",
            params={"key": DOODSTREAM_API_KEY},
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            _upload_server_url = resp["result"]
            return _upload_server_url
    except Exception as exc:
        print(f"  Upload server error: {exc}", file=sys.stderr)
    return None



def rename_dood_file(file_code: str, title: str) -> None:
    try:
        resp = requests.get(
            "https://doodapi.co/api/file/rename",
            params={"key": DOODSTREAM_API_KEY, "file_code": file_code, "title": title},
            timeout=15,
        ).json()
        if resp.get("status") == 200:
            print(f"  Title set: {title}")
        else:
            print(f"  Rename API returned: {resp}", file=sys.stderr)
    except Exception as exc:
        print(f"  Rename API error: {exc}", file=sys.stderr)



def upload_file(file_path: str, title: str, folder_id: str = "") -> str | None:
    if not DOODSTREAM_API_KEY:
        print("  DOODSTREAM_API_KEY missing - skipping upload", file=sys.stderr)
        return None
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  Uploading '{title}' ({size_mb} MB)...")
    for attempt in range(1, UPLOAD_RETRIES + 1):
        global _upload_server_url
        _upload_server_url = None
        server = get_upload_server()
        if not server:
            print(f"  [attempt {attempt}] No upload server", file=sys.stderr)
            time.sleep(RETRY_DELAY)
            continue
        try:
            with open(file_path, "rb") as fh:
                data = {"api_key": DOODSTREAM_API_KEY}
                if folder_id:
                    data["fld_id"] = folder_id
                resp = requests.post(
                    server,
                    files={"file": (os.path.basename(file_path), fh, "video/mp4")},
                    data=data,
                    timeout=ARIA2_TIMEOUT,
                ).json()
            if resp.get("status") == 200:
                result = (resp.get("result") or [{}])[0]
                file_code = result.get("file_code") or result.get("filecode") or ""
                url = result.get("embed_url") or result.get("download_url") or (f"https://doodstream.com/e/{file_code}" if file_code else "")
                if file_code:
                    rename_dood_file(file_code, title)
                return url or None
            print(f"  [attempt {attempt}] Bad response: {resp}", file=sys.stderr)
        except Exception as exc:
            print(f"  [attempt {attempt}] Exception: {exc}", file=sys.stderr)
        if attempt < UPLOAD_RETRIES:
            time.sleep(RETRY_DELAY)
    print(f"  All {UPLOAD_RETRIES} attempts failed for '{title}'", file=sys.stderr)
    return None


# HTML patching and git helpers


def patch_html_result(result: ProcessResult) -> bool:
    if not result.hs_url and not result.ss_url:
        return False
    html = read_html()
    if result.content_kind == "movie":
        if result.hs_url:
            html = patch_movie_hs(html, result.number, result.hs_url)
        if result.ss_url:
            html = patch_movie_ss(html, result.number, result.ss_url)
    elif result.content_kind == "special":
        html = patch_special_url(html, result.number, result.ss_url or result.hs_url or "")
    elif result.content_kind == "ova":
        html = patch_ova_url(html, result.number, result.ss_url or result.hs_url or "")
    else:
        if result.hs_url:
            html = patch_hs(html, result.number, result.hs_url)
        if result.ss_url:
            html = patch_ss(html, result.number, result.ss_url)
    write_html(html)
    return True



def git_has_changes() -> bool:
    result = subprocess.run(["git", "status", "--porcelain", HTML_FILE], capture_output=True, text=True, check=False)
    return bool(result.stdout.strip())



def _run_logged(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    return result



def git_commit_push(results: list[ProcessResult], sync_only: bool = False) -> None:
    if not git_has_changes():
        print("  No HTML changes to commit.")
        return
    ep_parts = sorted({str(r.number) for r in results if r.content_kind == "episode" and (r.hs_url or r.ss_url)}, key=int)
    movie_parts = sorted({f"M{r.number}" for r in results if r.content_kind == "movie" and (r.hs_url or r.ss_url)}, key=lambda v: int(v[1:]))
    special_parts = sorted({f"S{r.number}" for r in results if r.content_kind == "special" and (r.hs_url or r.ss_url)}, key=lambda v: int(v[1:]))
    ova_parts = sorted({f"OVA{r.number}" for r in results if r.content_kind == "ova" and (r.hs_url or r.ss_url)}, key=lambda v: int(v[3:]))
    if ep_parts or movie_parts or special_parts or ova_parts:
        message = "chore: add links for " + ", ".join(ep_parts + movie_parts + special_parts + ova_parts)
    else:
        message = "chore: auto sync index.html" if sync_only else "chore: update index.html"
    try:
        _run_logged(["git", "config", "user.email", "github-actions@github.com"])
        _run_logged(["git", "config", "user.name", "GitHub Actions"])
        _run_logged(["git", "add", HTML_FILE])
        commit_result = _run_logged(["git", "commit", "-m", message], check=False)
        if commit_result.returncode != 0:
            print("  Git commit skipped - nothing new to commit")
            return
        _run_logged(["git", "pull", "--rebase"], check=False)
        _run_logged(["git", "push"])
    except subprocess.CalledProcessError as exc:
        print(f"  Git error: {exc}", file=sys.stderr)


# Processing helpers


def process_video_file(video_file: str, subtitle_files: list[str]) -> ProcessResult:
    number, content_kind = parse_file_info(video_file, force_movie=MOVIE_MODE)
    if number is None:
        number = get_auto_episode()
        content_kind = "movie" if MOVIE_MODE else "episode"
        print(f"  Could not parse number from filename; using {content_kind} {number}")
    else:
        print(f"  Detected {content_kind} {number} from {os.path.basename(video_file)}")

    label_prefix = {"episode": "ep", "movie": "movie", "special": "special", "ova": "ova"}.get(content_kind, "ep")
    label = f"{label_prefix}_{number}"
    work_dir = make_work_dir(label)

    matched_subtitle = find_matching_external_subtitle(video_file, subtitle_files)
    if matched_subtitle:
        print(f"  Matched external subtitle: {matched_subtitle}")
    else:
        print("  No external subtitle match found; using embedded subtitles if available")

    ss_url = None
    hs_url = None
    ss_file = None
    hs_file = None
    try:
        ss_file = remux_to_mp4(video_file, work_dir, label)
        if ss_file:
            ss_title = format_dood_title(content_kind, number, "SS")
            ss_url = upload_file(ss_file, ss_title, SOFT_SUB_FOLDER_ID)
        hs_file = hardsub(video_file, work_dir, label, matched_subtitle)
        if hs_file:
            hs_title = format_dood_title(content_kind, number, "HS")
            hs_url = upload_file(hs_file, hs_title, HARD_SUB_FOLDER_ID)
    finally:
        cleanup_paths([ss_file or "", hs_file or "", video_file, work_dir])
        cleanup_empty_dirs(DOWNLOAD_ROOT)
        print(f"  Free space after cleanup: {get_free_gb():.2f} GiB")

    return ProcessResult(number=number, content_kind=content_kind, hs_url=hs_url, ss_url=ss_url)



def process_video_paths(video_paths: list[str], subtitle_files: list[str]) -> list[ProcessResult]:
    results: list[ProcessResult] = []
    for index, video in enumerate(video_paths, start=1):
        print(f"\n[{index}/{len(video_paths)}] Processing {os.path.basename(video)}")
        try:
            result = process_video_file(video, subtitle_files)
            results.append(result)
            if patch_html_result(result):
                git_commit_push([result])
        except Exception as exc:
            print(f"  FATAL ERROR while processing {video}: {exc}", file=sys.stderr)
    return results



def download_subtitle_magnets() -> list[str]:
    subtitle_files: list[str] = []
    if not SUBTITLE_MAGNET_LINKS:
        return subtitle_files
    magnets = parse_magnet_list(SUBTITLE_MAGNET_LINKS)
    print(f"Subtitle magnet mode: {len(magnets)} magnet(s)")
    for idx, magnet in enumerate(magnets, start=1):
        work_dir = make_work_dir(f"subtitle_{idx}")
        torrent_path = fetch_torrent_metadata(magnet)
        manifest = read_torrent_manifest(torrent_path) if torrent_path and os.path.exists(torrent_path) else None
        groups = build_select_groups_from_manifest(manifest, SUBTITLE_SELECT_FILES, SUBTITLE_EXTENSIONS)
        cleanup_path(os.path.dirname(torrent_path) if torrent_path else "")
        for g_idx, select_spec in enumerate(groups, start=1):
            group_dir = make_work_dir(f"subtitle_{idx}_{g_idx}")
            files, reason = download_magnet_once(magnet, select_spec, SUBTITLE_EXTENSIONS, group_dir)
            if files:
                subtitle_files.extend(files)
            else:
                print(f"  Subtitle magnet group failed: {reason}", file=sys.stderr)
                cleanup_path(group_dir)
    unique: list[str] = []
    seen: set[str] = set()
    for path in subtitle_files:
        norm = os.path.normpath(path)
        if norm not in seen and os.path.exists(norm):
            seen.add(norm)
            unique.append(norm)
    print(f"  Total subtitle files ready: {len(unique)}")
    return unique



def process_magnet_with_fallback(candidates: list[NyaaResult], subtitle_files: list[str]) -> list[ProcessResult]:
    for attempt_idx, candidate in enumerate(candidates, start=1):
        print(f"\nTrying source {attempt_idx}/{len(candidates)}: {candidate.title} | seeds={candidate.seeds}")
        torrent_path = fetch_torrent_metadata(candidate.magnet)
        manifest = read_torrent_manifest(torrent_path) if torrent_path and os.path.exists(torrent_path) else None
        groups = build_select_groups_from_manifest(manifest, SELECT_FILES, VIDEO_EXTENSIONS)
        cleanup_path(os.path.dirname(torrent_path) if torrent_path else "")
        all_results: list[ProcessResult] = []
        success_any = False
        failed_group = False
        for group_idx, select_spec in enumerate(groups, start=1):
            if storage_limit_hit():
                return all_results
            group_dir = make_work_dir(f"video_src_{attempt_idx}_{group_idx}")
            files, reason = download_magnet_once(candidate.magnet, select_spec, VIDEO_EXTENSIONS, group_dir)
            if not files:
                print(f"  Source failed on group {group_idx}: {reason}", file=sys.stderr)
                cleanup_path(group_dir)
                failed_group = True
                break
            success_any = True
            all_results.extend(process_video_paths(files, subtitle_files))
            cleanup_path(group_dir)
            cleanup_empty_dirs(DOWNLOAD_ROOT)
        if success_any and not failed_group:
            return all_results
        print("  Switching to next source candidate...")
    return []



def process_direct_magnets(magnets: list[str], subtitle_files: list[str]) -> list[ProcessResult]:
    all_results: list[ProcessResult] = []
    for idx, magnet in enumerate(magnets, start=1):
        print(f"\n[{idx}/{len(magnets)}] Direct magnet download")
        torrent_path = fetch_torrent_metadata(magnet)
        manifest = read_torrent_manifest(torrent_path) if torrent_path and os.path.exists(torrent_path) else None
        groups = build_select_groups_from_manifest(manifest, SELECT_FILES, VIDEO_EXTENSIONS)
        cleanup_path(os.path.dirname(torrent_path) if torrent_path else "")
        for group_idx, select_spec in enumerate(groups, start=1):
            if storage_limit_hit():
                return all_results
            group_dir = make_work_dir(f"direct_{idx}_{group_idx}")
            files, reason = download_magnet_once(magnet, select_spec, VIDEO_EXTENSIONS, group_dir)
            if not files:
                print(f"  Direct magnet group failed: {reason}", file=sys.stderr)
                cleanup_path(group_dir)
                continue
            all_results.extend(process_video_paths(files, subtitle_files))
            cleanup_path(group_dir)
            cleanup_empty_dirs(DOWNLOAD_ROOT)
    return all_results


# Bulk search mode


def run_anime_search_mode() -> None:
    limit_bytes = int(ANIME_BATCH_LIMIT_GB * (1024 ** 3))
    results = search_nyaa_all(ANIME_SEARCH_QUERY, ANIME_MAX_PAGES)
    if not results:
        print("No Nyaa results found for bulk anime search.", file=sys.stderr)
        sys.exit(1)
    batches = build_size_batches(results, limit_bytes)
    for idx, batch in enumerate(batches, start=1):
        total_gb = sum(item.size_bytes for item in batch) / (1024 ** 3)
        print(f"  Batch {idx}: {len(batch)} result(s), about {total_gb:.2f} GiB")
    if ANIME_BATCH_INDEX > len(batches):
        print(f"Requested batch {ANIME_BATCH_INDEX}, but only {len(batches)} batch(es) exist.", file=sys.stderr)
        sys.exit(1)
    selected = batches[ANIME_BATCH_INDEX - 1]
    for idx, item in enumerate(selected, start=1):
        if storage_limit_hit():
            break
        print(f"\n[{idx}/{len(selected)}] {item.title} | seeds={item.seeds}")
        work_dir = make_work_dir(f"bulk_{idx}")
        files, reason = download_magnet_once(item.magnet, validate_select_files(SELECT_FILES), VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS, work_dir)
        if not files:
            print(f"  Bulk item failed: {reason}", file=sys.stderr)
            cleanup_path(work_dir)
            continue
        print(f"  Downloaded {len(files)} file(s) to {work_dir}")
    print("\nBulk anime search batch finished.")


# Main


def run_sync_only() -> None:
    patched = bulk_sync()
    if patched:
        git_commit_push([], sync_only=True)



def main() -> None:
    ensure_dirs()

    if ANIME_SEARCH_QUERY:
        run_anime_search_mode()
        return

    subtitle_files = download_subtitle_magnets()

    results: list[ProcessResult] = []
    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Direct magnet mode: {len(magnets)} magnet(s)")
        results.extend(process_direct_magnets(magnets, subtitle_files))
    else:
        episodes = parse_episode_override(EPISODE_OVERRIDE)
        print(f"Episode search mode: {episodes}")
        for episode in episodes:
            if storage_limit_hit():
                break
            print(f"\nSearching sources for episode {episode}...")
            candidates = search_nyaa_candidates(episode, "movie" if MOVIE_MODE else "episode")
            if not candidates:
                print(f"  No Nyaa sources found for {episode}", file=sys.stderr)
                continue
            results.extend(process_magnet_with_fallback(candidates, subtitle_files))

    if not results:
        print("No files were processed. Running bulk sync only.")
        run_sync_only()
        return

    print("\nRun summary:")
    for result in results:
        print(
            f"  {result.content_kind.upper():>7} {result.number:>4} "
            f"SS:{'OK' if result.ss_url else 'FAIL'} HS:{'OK' if result.hs_url else 'FAIL'}"
        )

    cleanup_paths(subtitle_files)
    cleanup_empty_dirs(DOWNLOAD_ROOT)


if __name__ == "__main__":
    main()
