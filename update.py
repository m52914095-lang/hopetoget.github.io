"""
update.py - Patch index.html with DoodStream links and bulk sync from the Dood API.
"""

import argparse
import json
import os
import re
import sys
from typing import Any

import requests

from conan_utils import content_sort_key, parse_dood_title

DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "")
HTML_FILE = os.environ.get("HTML_FILE", "index.html")
DOOD_API_BASE = os.environ.get("DOOD_API_BASE", "https://doodapi.co/api")


def read_html() -> str:
    with open(HTML_FILE, "r", encoding="utf-8") as fh:
        return fh.read()


def write_html(content: str) -> None:
    with open(HTML_FILE, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(f"  Saved {HTML_FILE}")


def _episode_line_re(ep: int) -> re.Pattern[str]:
    return re.compile(rf"^(?P<indent>\s*)EP_DB\[{ep}\]\s*=\s*(?P<obj>\{{.*?\}});\s*$", re.MULTILINE)


def _load_episode_obj(obj_text: str) -> dict[str, Any]:
    try:
        return json.loads(obj_text)
    except json.JSONDecodeError:
        normalized = re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*:", r'"\1":', obj_text)
        return json.loads(normalized)


def _insert_episode_line(html: str, ep: int, obj: dict[str, Any]) -> str:
    line = f"    EP_DB[{ep}] = {json.dumps(obj, ensure_ascii=True)};"
    last_match = None
    for match in re.finditer(r"^\s*EP_DB\[(\d+)\]\s*=\s*\{.*?\};\s*$", html, re.MULTILINE):
        last_match = match
    if last_match:
        insert_at = last_match.end()
        return html[:insert_at] + "\n" + line + html[insert_at:]

    anchor = re.search(r"^\s*function\s+hasEpisodeLink\s*\(", html, re.MULTILINE)
    if anchor:
        return html[:anchor.start()] + line + "\n" + html[anchor.start():]
    return html + "\n" + line + "\n"


def _update_episode_original_field(html: str, ep: int, field: str, url: str) -> str:
    line_re = _episode_line_re(ep)
    match = line_re.search(html)
    if not match:
        obj = {"original": {field: url}, "remastered": {}}
        print(f"  [EP {field.upper()}] Inserted episode {ep}")
        return _insert_episode_line(html, ep, obj)

    obj = _load_episode_obj(match.group("obj"))
    obj.setdefault("original", {})
    obj.setdefault("remastered", {})
    obj["original"][field] = url

    indent = match.group("indent")
    new_line = f"{indent}EP_DB[{ep}] = {json.dumps(obj, ensure_ascii=True)};"
    print(f"  [EP {field.upper()}] Updated episode {ep}")
    return html[:match.start()] + new_line + html[match.end():]


def patch_hs(html: str, ep: int, url: str) -> str:
    return _update_episode_original_field(html, ep, "hard", url)


def patch_ss(html: str, ep: int, url: str) -> str:
    return _update_episode_original_field(html, ep, "soft", url)


def _movie_pattern(num: int, mode: str, field: str) -> re.Pattern[str]:
    return re.compile(
        rf'^([ \t]*MOVIE_DB\[{num}\]\.{mode}\.{field}\s*=\s*)"[^"]*"(;.*)?\s*$',
        re.MULTILINE,
    )


def _movie_anchor(html: str) -> int:
    anchor = re.search(r"^\s*const\s+EP_DB\s*=\s*\{\};\s*$", html, re.MULTILINE)
    if anchor:
        return anchor.start()
    last_match = None
    for last_match in re.finditer(r"^\s*MOVIE_DB\[\d+\]\.(?:original|remastered)\.(?:hard|soft|dub)\s*=.*$", html, re.MULTILINE):
        pass
    if last_match:
        return last_match.end() + 1
    return len(html)


def _patch_movie_field(html: str, num: int, mode: str, field: str, url: str, label: str) -> str:
    pat = _movie_pattern(num, mode, field)
    new_line = f'    MOVIE_DB[{num}].{mode}.{field} = "{url}"; // Movie {num} {label}'
    if pat.search(html):
        print(f"  [MV {label}] Updated movie {num}")
        return pat.sub(new_line, html)
    print(f"  [MV {label}] Inserted movie {num}")
    anchor = _movie_anchor(html)
    return html[:anchor] + new_line + "\n" + html[anchor:]


def patch_movie_hs(html: str, num: int, url: str) -> str:
    return _patch_movie_field(html, num, "original", "hard", url, "HS")


def patch_movie_ss(html: str, num: int, url: str) -> str:
    return _patch_movie_field(html, num, "original", "soft", url, "SS")


def _patch_array_url(html: str, array_name: str, num: int, url: str, label: str) -> str:
    pattern = re.compile(
        rf'({array_name}\s*=\s*\[.*?\{{\s*id:{num},.*?url:\")([^\"]*)(\".*?\}})',
        re.DOTALL,
    )
    if pattern.search(html):
        print(f"  [{label}] Updated {array_name.lower()} {num}")
        return pattern.sub(rf"\1{url}\3", html, count=1)
    raise ValueError(f"Could not find {array_name} entry {num}")


def patch_special_url(html: str, num: int, url: str) -> str:
    if not url:
        return html
    return _patch_array_url(html, "SPECIALS", num, url, "SPECIAL")


def patch_ova_url(html: str, num: int, url: str) -> str:
    if not url:
        return html
    return _patch_array_url(html, "OVAS", num, url, "OVA")


def apply_patch(
    ep: int | None = None,
    movie: int | None = None,
    special: int | None = None,
    ova: int | None = None,
    hs_url: str | None = None,
    ss_url: str | None = None,
) -> None:
    if not hs_url and not ss_url:
        print("Nothing to patch.")
        return

    html = read_html()
    if ep is not None:
        if hs_url:
            html = patch_hs(html, ep, hs_url)
        if ss_url:
            html = patch_ss(html, ep, ss_url)
    elif movie is not None:
        if hs_url:
            html = patch_movie_hs(html, movie, hs_url)
        if ss_url:
            html = patch_movie_ss(html, movie, ss_url)
    elif special is not None:
        html = patch_special_url(html, special, ss_url or hs_url or "")
    elif ova is not None:
        html = patch_ova_url(html, ova, ss_url or hs_url or "")

    write_html(html)


def dood_fetch_json(url: str, **params: Any) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data.get("status") != 200:
        raise RuntimeError(data.get("msg") or f"Dood API error for {url}")
    return data


def fetch_all_dood_files_recursive() -> list[dict[str, Any]]:
    if not DOODSTREAM_API_KEY:
        return []

    out: list[dict[str, Any]] = []

    def visit_folder(folder_id: str = "0", path: str = "Root") -> None:
        data = dood_fetch_json(f"{DOOD_API_BASE}/folder/list", key=DOODSTREAM_API_KEY, fld_id=folder_id)
        result = data.get("result") or {}
        for row in result.get("files") or []:
            file_code = row.get("file_code") or row.get("filecode") or ""
            embed_url = row.get("embed_url") or (f"https://doodstream.com/e/{file_code}" if file_code else "")
            out.append(
                {
                    "title": row.get("title") or "",
                    "folder_path": path,
                    "file_code": file_code,
                    "embed_url": embed_url,
                    "download_url": row.get("download_url") or "",
                }
            )
        for folder in result.get("folders") or []:
            child_name = folder.get("name") or "Folder"
            child_id = str(folder.get("fld_id") or folder.get("id") or "0")
            child_path = child_name if path == "Root" else f"{path} / {child_name}"
            visit_folder(child_id, child_path)

    try:
        visit_folder("0", "Root")
    except Exception as exc:
        print(f"  Dood folder recursion failed: {exc}", file=sys.stderr)

    if out:
        return out

    page = 1
    while True:
        try:
            data = dood_fetch_json(f"{DOOD_API_BASE}/file/list", key=DOODSTREAM_API_KEY, page=page, per_page=200)
        except Exception as exc:
            print(f"  Dood file list failed on page {page}: {exc}", file=sys.stderr)
            break
        result = data.get("result") or {}
        rows = result.get("results") or []
        if not rows:
            break
        for row in rows:
            file_code = row.get("file_code") or row.get("filecode") or ""
            embed_url = row.get("embed_url") or (f"https://doodstream.com/e/{file_code}" if file_code else "")
            out.append(
                {
                    "title": row.get("title") or "",
                    "folder_path": row.get("folder_path") or "Root",
                    "file_code": file_code,
                    "embed_url": embed_url,
                    "download_url": row.get("download_url") or "",
                }
            )
        if page >= int(result.get("pages", 1) or 1):
            break
        page += 1
    return out


def bulk_sync() -> int:
    if not DOODSTREAM_API_KEY:
        print("DOODSTREAM_API_KEY missing - skipping bulk sync")
        return 0

    print("Fetching all DoodStream files...")
    files = fetch_all_dood_files_recursive()
    print(f"  Found {len(files)} total files")

    grouped: dict[tuple[str, int], dict[str, str]] = {}
    labels: dict[tuple[str, int], str] = {}

    for row in files:
        title = (row.get("title") or "").strip()
        parsed = parse_dood_title(title)
        if not parsed:
            continue
        url = row.get("embed_url") or row.get("download_url") or ""
        if not url:
            continue
        key = (parsed["content_kind"], parsed["number"])
        grouped.setdefault(key, {})[parsed["sub_kind"]] = url
        labels[key] = parsed["title"]

    ordered = sorted(
        [(kind, number, labels[(kind, number)], urls) for (kind, number), urls in grouped.items()],
        key=lambda item: content_sort_key((item[0], item[1], "SS" if "SS" in item[3] else "HS", item[2])),
    )

    html = read_html()
    patched = 0
    for content_kind, number, _title, urls in ordered:
        if content_kind == "movie":
            if urls.get("HS"):
                html = patch_movie_hs(html, number, urls["HS"])
                patched += 1
            if urls.get("SS"):
                html = patch_movie_ss(html, number, urls["SS"])
                patched += 1
        elif content_kind == "special":
            chosen = urls.get("SS") or urls.get("HS") or urls.get("DUB")
            if chosen:
                html = patch_special_url(html, number, chosen)
                patched += 1
        elif content_kind == "ova":
            chosen = urls.get("SS") or urls.get("HS") or urls.get("DUB")
            if chosen:
                html = patch_ova_url(html, number, chosen)
                patched += 1
        else:
            if urls.get("HS"):
                html = patch_hs(html, number, urls["HS"])
                patched += 1
            if urls.get("SS"):
                html = patch_ss(html, number, urls["SS"])
                patched += 1
            if urls.get("DUB"):
                html = _update_episode_original_field(html, number, "dub", urls["DUB"])
                patched += 1

    if patched:
        write_html(html)
        print(f"  Bulk sync complete - {patched} field(s) updated")
    else:
        print("  No matching files found")
    return patched


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch index.html with DoodStream links")
    parser.add_argument("--ep", type=int, help="Episode number")
    parser.add_argument("--movie", type=int, help="Movie number")
    parser.add_argument("--special", type=int, help="Special number")
    parser.add_argument("--ova", type=int, help="OVA number")
    parser.add_argument("--hs", metavar="URL", help="Hard-sub URL")
    parser.add_argument("--ss", metavar="URL", help="Soft-sub URL")
    parser.add_argument("--bulk-sync", action="store_true", help="Sync all files from DoodStream API")
    args = parser.parse_args()

    if args.bulk_sync:
        bulk_sync()
    elif any(value is not None for value in (args.ep, args.movie, args.special, args.ova)):
        apply_patch(ep=args.ep, movie=args.movie, special=args.special, ova=args.ova, hs_url=args.hs, ss_url=args.ss)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
