import base64
import hashlib
import os
import re
from typing import Any

XOR_KEY = "DetectiveConan2024"
TITLE_KIND_ALIASES = {
    "": "episode",
    "movie": "movie",
    "special": "special",
    "ova": "ova",
}

NEW_TITLE_RE = re.compile(
    r"^\s*Detective\s+Conan(?:\s+(Movie|Special|OVA))?\s*-\s*(HS|SS|DUB)\s*\(?\s*(\d{1,4})\s*\)?\s*$",
    re.IGNORECASE,
)
OLD_TITLE_RE = re.compile(
    r"^\s*Detective\s+Conan(?:\s+(Movie|Special|OVA))?\s*-\s*(\d{1,4})\s+(HS|SS|DUB)\s*$",
    re.IGNORECASE,
)


def xor_encrypt(data: str, key: str = XOR_KEY) -> str:
    key_bytes = [ord(c) for c in key]
    encrypted = bytes(ord(c) ^ key_bytes[i % len(key_bytes)] for i, c in enumerate(data))
    return base64.b64encode(encrypted).decode("ascii")


def xor_decrypt(enc: str, key: str = XOR_KEY) -> str:
    key_bytes = [ord(c) for c in key]
    raw = base64.b64decode(enc)
    return "".join(chr(b ^ key_bytes[i % len(key_bytes)]) for i, b in enumerate(raw))


def hash_password(password: str, key: str = "ConanEncryptKey2024") -> str:
    sha = hashlib.sha256(password.encode()).hexdigest()
    return xor_encrypt(sha, key)


def format_dood_title(content_kind: str, number: int, sub_kind: str) -> str:
    sub = sub_kind.upper()
    if content_kind == "movie":
        return f"Detective Conan Movie - {sub} ({number})"
    if content_kind == "special":
        return f"Detective Conan Special - {sub} ({number})"
    if content_kind == "ova":
        return f"Detective Conan OVA - {sub} ({number})"
    return f"Detective Conan - {sub} ({number})"


def parse_dood_title(title: str) -> dict[str, Any] | None:
    text = (title or "").strip()
    match = NEW_TITLE_RE.match(text)
    if match:
        raw_kind = (match.group(1) or "").strip().lower()
        return {
            "content_kind": TITLE_KIND_ALIASES.get(raw_kind, "episode"),
            "number": int(match.group(3)),
            "sub_kind": match.group(2).upper(),
            "title": text,
        }

    match = OLD_TITLE_RE.match(text)
    if match:
        raw_kind = (match.group(1) or "").strip().lower()
        return {
            "content_kind": TITLE_KIND_ALIASES.get(raw_kind, "episode"),
            "number": int(match.group(2)),
            "sub_kind": match.group(3).upper(),
            "title": text,
        }

    return None


def content_sort_key(item: tuple[str, int, str, str]) -> tuple[int, int, int, str]:
    content_kind, number, sub_kind, title = item
    kind_order = {"episode": 0, "movie": 1, "special": 2, "ova": 3}.get(content_kind, 9)
    sub_order = {"SS": 0, "DUB": 1, "HS": 2}.get(sub_kind.upper(), 9)
    return kind_order, int(number), sub_order, title.lower()


VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".m4v", ".mov", ".ts", ".m2ts"}
SUBTITLE_EXTENSIONS = {".ass", ".ssa", ".srt", ".vtt", ".sub", ".sup"}
ZIP_EXTENSIONS = {".zip"}


def parse_file_info(filename: str, force_movie: bool = False) -> tuple[int | None, str]:
    base = os.path.basename(filename)
    lower = base.lower()

    if force_movie:
        match = re.search(r"\bmovie\s*[- ]?\s*(\d{1,3})\b", lower)
        if not match:
            match = re.search(r"\b(\d{1,3})\b", lower)
        return (int(match.group(1)) if match else None, "movie")

    if re.search(r"\b(?:ova|magic file|bonus file)\b", lower):
        match = re.search(r"\b(?:ova|magic file|bonus file)\s*[- ]?\s*(\d{1,3})\b", lower)
        if not match:
            match = re.search(r"\b(\d{1,3})\b", lower)
        return (int(match.group(1)) if match else None, "ova")

    if re.search(r"\bspecial\b", lower):
        match = re.search(r"\bspecial\s*[- ]?\s*(\d{1,3})\b", lower)
        if not match:
            match = re.search(r"\b(\d{1,3})\b", lower)
        return (int(match.group(1)) if match else None, "special")

    if re.search(r"\b(?:movie|film)\b", lower):
        match = re.search(r"\b(?:movie|film)\s*[- ]?\s*(\d{1,3})\b", lower)
        if not match:
            match = re.search(r"\b(\d{1,3})\b", lower)
        return (int(match.group(1)) if match else None, "movie")

    patterns = [
        r"Detective Conan\s*[- ]\s*(\d{3,4})\b",
        r"Case Closed\s*[- ]?\s*(\d{3,4})\b",
        r"\b(?:ep|episode|e)\s*(\d{3,4})\b",
        r"\[(\d{3,4})\]",
        r"\b(\d{3,4})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, base, re.IGNORECASE)
        if match:
            return int(match.group(1)), "episode"

    return None, "episode"


def parse_select_spec(raw: str) -> list[int]:
    raw = (raw or "").strip()
    if not raw:
        return []
    if not re.fullmatch(r"[0-9,\- ]+", raw):
        return []
    values: list[int] = []
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
                continue
            if start > end:
                start, end = end, start
            values.extend(range(start, end + 1))
        else:
            try:
                values.append(int(part))
            except ValueError:
                continue
    out: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value > 0 and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def compress_select_spec(indexes: list[int]) -> str:
    if not indexes:
        return ""
    seq = sorted(set(int(x) for x in indexes if int(x) > 0))
    if not seq:
        return ""
    ranges: list[str] = []
    start = prev = seq[0]
    for value in seq[1:]:
        if value == prev + 1:
            prev = value
            continue
        ranges.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = value
    ranges.append(f"{start}-{prev}" if start != prev else str(start))
    return ",".join(ranges)


class BencodeError(ValueError):
    pass


def _bdecode(data: bytes, index: int = 0) -> tuple[Any, int]:
    if index >= len(data):
        raise BencodeError("unexpected end of data")
    token = data[index:index + 1]

    if token == b"i":
        end = data.index(b"e", index)
        return int(data[index + 1:end]), end + 1

    if token == b"l":
        index += 1
        items = []
        while data[index:index + 1] != b"e":
            item, index = _bdecode(data, index)
            items.append(item)
        return items, index + 1

    if token == b"d":
        index += 1
        obj: dict[bytes, Any] = {}
        while data[index:index + 1] != b"e":
            key, index = _bdecode(data, index)
            value, index = _bdecode(data, index)
            if not isinstance(key, bytes):
                raise BencodeError("dictionary key must be bytes")
            obj[key] = value
        return obj, index + 1

    if token.isdigit():
        colon = data.index(b":", index)
        length = int(data[index:colon])
        start = colon + 1
        end = start + length
        return data[start:end], end

    raise BencodeError(f"invalid token {token!r}")


def bdecode(data: bytes) -> Any:
    value, index = _bdecode(data, 0)
    if index != len(data):
        raise BencodeError("trailing data")
    return value


def _to_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def read_torrent_manifest(torrent_path: str) -> dict[str, Any]:
    with open(torrent_path, "rb") as fh:
        meta = bdecode(fh.read())

    if not isinstance(meta, dict):
        raise BencodeError("torrent root is not a dictionary")
    info = meta.get(b"info")
    if not isinstance(info, dict):
        raise BencodeError("torrent info section missing")

    files: list[dict[str, Any]] = []
    total_size = 0

    if b"files" in info:
        for idx, entry in enumerate(info[b"files"], start=1):
            if not isinstance(entry, dict):
                continue
            length = int(entry.get(b"length", 0) or 0)
            parts = entry.get(b"path", [])
            path = "/".join(_to_text(p) for p in parts)
            files.append({"index": idx, "path": path, "length": length})
            total_size += length
    else:
        name = _to_text(info.get(b"name", b"unnamed"))
        length = int(info.get(b"length", 0) or 0)
        files.append({"index": 1, "path": name, "length": length})
        total_size += length

    return {
        "name": _to_text(info.get(b"name", b"unnamed")),
        "files": files,
        "total_size": total_size,
    }
