import os
import re
import json
import hashlib
import threading
import atexit
from collections import Counter, defaultdict
from urllib.parse import urlparse, urldefrag, urljoin, urlunparse

from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Allowed crawl scope (per assignment spec)
# ---------------------------------------------------------------------------

ALLOWED_HOST_SUFFIXES = (
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
)
ALLOWED_HOSTS_EXACT = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)


# ---------------------------------------------------------------------------
# Filters: file extensions and trap path / query patterns
# ---------------------------------------------------------------------------

DISALLOWED_EXT = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico"
    r"|png|tiff?|mid|mp2|mp3|mp4"
    r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
    r"|ps|eps|tex|ppt|pptx|ppsx|pps|key|odp"
    r"|doc|docx|odt|xls|xlsx|ods|names"
    r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
    r"|epub|dll|cnf|tgz|sha1|apk|war|ear"
    r"|thmx|mso|arff|rtf|jar|csv|tsv"
    r"|rm|smil|wmv|swf|wma|zip|rar|gz|z|lz|xz"
    r"|mat|nb|cdf|sql|sqlite|db"
    r"|svg|webp|webm|mka|m4a|aac|flac|opus"
    r"|woff2?|ttf|eot|otf"
    r"|ipynb|c|cpp|h|py|java|class)$"
)

TRAP_PATH_PATTERN = re.compile(
    r"(/calendar(?:/|$)|/events?/\d{4}|"
    r"/\d{4}/\d{2}/\d{2}(?:/|$)|/\d{4}-\d{2}-\d{2}(?:/|$)|"
    r"/page/\d{3,}(?:/|$)|"
    r"/wp-(?:login|admin|json)|"
    r"/feed/?$|/atom/?$|/rss/?$|"
    r"/(?:trackback|pingback)/?$|"
    r"/files/|/sampledata/|/supplement/)",
    re.IGNORECASE,
)

TRAP_QUERY_PATTERN = re.compile(
    r"(?:^|&)(?:phpsessid|jsessionid|sessionid|sid="
    r"|share=|replytocom="
    r"|action=(?:login|edit|history|raw|diff|source|print|download|rss)"
    r"|do=(?:login|edit|diff|revisions|media|export|backlink)"
    r"|format=(?:txt|xml|atom|rss|ical|ics)"
    r"|ical=|outlook-ical=|tribe-bar-date="
    r"|redirect_to=|redirect=|return=|returnto=)",
    re.IGNORECASE,
)

DROP_QUERY_KEYS = frozenset({
    "phpsessid", "jsessionid", "sid", "sessionid",
    "utm_source", "utm_medium", "utm_campaign",
    "utm_term", "utm_content", "fbclid", "gclid",
    "share", "replytocom",
})


# ---------------------------------------------------------------------------
# Stop-words list (from the assignment link)
# ---------------------------------------------------------------------------

STOP_WORDS = frozenset([
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can't",
    "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
    "doing", "don't", "down", "during", "each", "few", "for", "from",
    "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
    "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
    "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've",
    "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's",
    "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of",
    "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd",
    "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
    "that", "that's", "the", "their", "theirs", "them", "themselves", "then",
    "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves",
])


# ---------------------------------------------------------------------------
# Tunable heuristics
# ---------------------------------------------------------------------------

MAX_URL_LENGTH = 300
MAX_PATH_DEPTH = 12
MAX_REPEATED_SEGMENT = 2
MIN_PAGE_WORDS = 50
MAX_PAGE_BYTES = 8 * 1024 * 1024


# ---------------------------------------------------------------------------
# Persistent analytics — answers the four report questions across restarts
# ---------------------------------------------------------------------------

ANALYTICS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "analytics.json"
)
_FLUSH_INTERVAL = 25
_lock = threading.Lock()
_pending_writes = 0


def _empty_state():
    return {
        "unique_urls": set(),
        "longest": {"url": None, "words": 0},
        "word_freq": Counter(),
        "subdomain_pages": defaultdict(set),
        "content_hashes": set(),
    }


def _load_state():
    if not os.path.exists(ANALYTICS_FILE):
        return _empty_state()
    try:
        with open(ANALYTICS_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_state()
    state = _empty_state()
    state["unique_urls"] = set(data.get("unique_urls", []))
    state["longest"] = data.get("longest", {"url": None, "words": 0})
    state["word_freq"] = Counter(data.get("word_freq", {}))
    state["subdomain_pages"] = defaultdict(
        set, {k: set(v) for k, v in data.get("subdomain_pages", {}).items()}
    )
    state["content_hashes"] = set(data.get("content_hashes", []))
    return state


_STATE = _load_state()


def _flush_state():
    payload = {
        "unique_urls": sorted(_STATE["unique_urls"]),
        "longest": _STATE["longest"],
        "word_freq": dict(_STATE["word_freq"]),
        "subdomain_pages": {
            k: sorted(v) for k, v in _STATE["subdomain_pages"].items()
        },
        "content_hashes": list(_STATE["content_hashes"]),
    }
    tmp = ANALYTICS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, ANALYTICS_FILE)


atexit.register(lambda: _flush_state() if _STATE["unique_urls"] else None)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def normalize(url):
    """Defragment + canonicalise so two equivalent URLs hash equal."""
    if not url:
        return None
    try:
        url, _ = urldefrag(url)
        parsed = urlparse(url)
    except ValueError:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    host = (parsed.hostname or "").lower()
    if not host:
        return None
    port = ""
    try:
        if parsed.port is not None and parsed.port not in (80, 443):
            port = f":{parsed.port}"
    except ValueError:
        return None
    netloc = f"{host}{port}"
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    query = parsed.query
    if query:
        kept = []
        for piece in query.split("&"):
            key = piece.split("=", 1)[0].lower()
            if key in DROP_QUERY_KEYS:
                continue
            kept.append(piece)
        query = "&".join(kept)
    return urlunparse((parsed.scheme, netloc, path, parsed.params, query, ""))


def _host_allowed(host):
    if not host:
        return False
    host = host.lower()
    if host in ALLOWED_HOSTS_EXACT:
        return True
    return any(host.endswith(suffix) for suffix in ALLOWED_HOST_SUFFIXES)


def _looks_like_trap(parsed):
    if len(parsed.geturl()) > MAX_URL_LENGTH:
        return True
    path = parsed.path or ""
    if TRAP_PATH_PATTERN.search(path):
        return True
    if parsed.query and TRAP_QUERY_PATTERN.search(parsed.query):
        return True
    segments = [s for s in path.split("/") if s]
    if len(segments) > MAX_PATH_DEPTH:
        return True
    counts = Counter(segments)
    if any(c > MAX_REPEATED_SEGMENT for c in counts.values()):
        return True
    for i in range(len(segments) - 2):
        if segments[i] == segments[i + 1] == segments[i + 2]:
            return True
    return False


_WORD_RE = re.compile(r"[A-Za-z][A-Za-z']{1,}")


def _tokenize(text):
    return [w.lower() for w in _WORD_RE.findall(text)]


# ---------------------------------------------------------------------------
# Crawler entry points
# ---------------------------------------------------------------------------

def scraper(url, resp):
    links = extract_next_links(url, resp)
    out, seen = [], set()
    for raw in links:
        normed = normalize(raw)
        if not normed or normed in seen:
            continue
        seen.add(normed)
        if is_valid(normed):
            out.append(normed)
    return out


def extract_next_links(url, resp):
    if resp is None or resp.status != 200:
        return []
    raw = getattr(resp, "raw_response", None)
    if raw is None or not getattr(raw, "content", None):
        return []
    content = raw.content
    if len(content) > MAX_PAGE_BYTES:
        return []

    headers = getattr(raw, "headers", None) or {}
    ctype = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
    if ctype and "html" not in ctype and "xml" not in ctype:
        return []

    final_url = getattr(raw, "url", None) or url

    try:
        soup = BeautifulSoup(content, "html.parser")
    except Exception:
        return []

    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = soup.get_text(" ", strip=True)
    tokens = _tokenize(text)
    word_count = len(tokens)

    base_url = final_url
    base_tag = soup.find("base", href=True)
    if base_tag:
        try:
            base_url = urljoin(final_url, base_tag["href"])
        except ValueError:
            pass

    if word_count < MIN_PAGE_WORDS:
        _record_visit(final_url, 0, None, None)
        return _harvest_links(soup, base_url)

    content_hash = hashlib.md5(" ".join(tokens).encode("utf-8")).hexdigest()
    _record_visit(final_url, word_count, tokens, content_hash)
    return _harvest_links(soup, base_url)


def _harvest_links(soup, base_url):
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(
            ("javascript:", "mailto:", "tel:", "#", "data:", "ftp:", "file:")
        ):
            continue
        try:
            out.append(urljoin(base_url, href))
        except ValueError:
            continue
    return out


def _record_visit(url, word_count, tokens, content_hash):
    global _pending_writes
    normed = normalize(url)
    if not normed:
        return
    host = (urlparse(normed).hostname or "").lower()

    with _lock:
        if content_hash and content_hash in _STATE["content_hashes"]:
            tokens = None
        elif content_hash:
            _STATE["content_hashes"].add(content_hash)

        is_new = normed not in _STATE["unique_urls"]
        _STATE["unique_urls"].add(normed)

        if is_new and host.endswith("uci.edu"):
            sub_host = host[4:] if host.startswith("www.") else host
            _STATE["subdomain_pages"][sub_host].add(normed)

        if word_count > _STATE["longest"]["words"]:
            _STATE["longest"] = {"url": normed, "words": word_count}

        if tokens:
            _STATE["word_freq"].update(
                w for w in tokens
                if w not in STOP_WORDS and len(w) > 1
            )

        _pending_writes += 1
        if _pending_writes >= _FLUSH_INTERVAL:
            try:
                _flush_state()
                _pending_writes = 0
            except OSError:
                pass


def is_valid(url):
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    if not _host_allowed(host):
        return False
    if DISALLOWED_EXT.match((parsed.path or "").lower()):
        return False
    if _looks_like_trap(parsed):
        return False
    return True


# ---------------------------------------------------------------------------
# `python3 scraper.py` -> dump report from analytics.json
# ---------------------------------------------------------------------------

def print_report(top_n=50):
    state = _load_state()
    print(f"Unique pages: {len(state['unique_urls'])}")
    longest = state["longest"]
    print(f"Longest page: {longest['url']} ({longest['words']} words)\n")
    print(f"Top {top_n} words:")
    for word, count in state["word_freq"].most_common(top_n):
        print(f"  {word}\t{count}")
    print(f"\nSubdomains under uci.edu:")
    for sub in sorted(state["subdomain_pages"]):
        print(f"  {sub}, {len(state['subdomain_pages'][sub])}")


if __name__ == "__main__":
    print_report()
