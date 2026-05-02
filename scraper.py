import json
import os
import re
from hashlib import md5
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Immutable configuration (constants, not mutable globals)
# ---------------------------------------------------------------------------

STOPWORDS = frozenset({
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can't",
    "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
    "doing", "don't", "down", "during", "each", "few", "for", "from",
    "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
    "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its",
    "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other",
    "ought", "our", "ours", "ourselves", "out", "over", "own", "same",
    "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
    "so", "some", "such", "than", "that", "that's", "the", "their",
    "theirs", "them", "themselves", "then", "there", "there's", "these",
    "they", "they'd", "they'll", "they're", "they've", "this", "those",
    "through", "to", "too", "under", "until", "up", "very", "was", "wasn't",
    "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
    "what's", "when", "when's", "where", "where's", "which", "while",
    "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
    "yours", "yourself", "yourselves",
    "obj", "endobj", "stream", "endstream", "flatedecode",
})

ALLOWED_DOMAIN_SUFFIXES = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

MIN_TOKENS_FOR_PAGE = 150
MAX_URL_LEN = 280
MAX_PATH_DEPTH = 12
MAX_REPEATED_SEGMENT = 2
REPORT_FLUSH_EVERY = 30

_HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(_HERE, "crawler_state.json")
REPORT_PATH = os.path.join(_HERE, "crawler_report.txt")

_TOKEN_RE = re.compile(r"[a-z][a-z']+")
_TRAP_PATTERNS = (
    re.compile(r"/pix/", re.IGNORECASE),
    re.compile(r"/junkyard/", re.IGNORECASE),
    re.compile(r"/calendar(?:/|$)", re.IGNORECASE),
    re.compile(r"/events?/.*\d{4}", re.IGNORECASE),
    re.compile(r"/\d{4}/\d{2}/\d{2}(?:/|$)"),
    re.compile(r"/\d{4}-\d{2}-\d{2}(?:/|$)"),
    re.compile(r"tribe-bar-date=", re.IGNORECASE),
    re.compile(r"eventDate=", re.IGNORECASE),
    re.compile(r"(?:^|&)ical=", re.IGNORECASE),
    re.compile(r"/page/\d{3,}(?:/|$)"),
    re.compile(r"/feed/?$", re.IGNORECASE),
    re.compile(r"/(?:atom|rss)/?$", re.IGNORECASE),
    re.compile(r"/genealogy/", re.IGNORECASE), # Avoid pages like https://ics.uci.edu/~dhirschb/genealogy/Krakow/Families/Bader.html#Gele
    re.compile(r"family[-_ ]?listing", re.IGNORECASE), #
)
_DOWNLOAD_BUCKETS = (
    "/files/", "/sampledata/", "/supplement/",
    "/wp-content/uploads/", "/raw-attachment/",
)
_EXTENSION_BLACKLIST = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico"
    r"|png|tiff?|mid|mp2|mp3|mp4"
    r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
    r"|ps|eps|tex|ppt|pptx|ppsx|pps|key|odp"
    r"|doc|docx|odt|xls|xlsx|ods|names"
    r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
    r"|epub|dll|cnf|tgz|sha1|apk|war|ear|jar"
    r"|thmx|mso|arff|rtf|csv|tsv|sql|sqlite|db"
    r"|svg|webp|webm|m4a|aac|flac|opus"
    r"|woff2?|ttf|eot|otf"
    r"|ipynb|cpp|java|class|odc|txt|img|mpg|scm|bib|ff|ma"
    r"|rm|smil|wmv|swf|wma|zip|rar|gz|z|lz|xz)$"
)


# ---------------------------------------------------------------------------
# State persistence — disk IS the only mutable store
# ---------------------------------------------------------------------------

def _empty_state():
    return {
        "crawled_urls": set(),
        "skipped_urls": set(),
        "content_signatures": set(),
        "word_counter": {},
        "subdomain_pages": {},
        "longest_url": "",
        "longest_count": 0,
    }


def _load_state():
    if not os.path.exists(STATE_PATH):
        return _empty_state()
    try:
        with open(STATE_PATH, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_state()
    return {
        "crawled_urls": set(data.get("crawled_urls", [])),
        "skipped_urls": set(data.get("skipped_urls", [])),
        "content_signatures": set(data.get("content_signatures", [])),
        "word_counter": dict(data.get("word_counter", {})),
        "subdomain_pages": {
            k: set(v) for k, v in data.get("subdomain_pages", {}).items()
        },
        "longest_url": data.get("longest_url", ""),
        "longest_count": data.get("longest_count", 0),
    }


def _save_state(state):
    payload = {
        "crawled_urls": sorted(state["crawled_urls"]),
        "skipped_urls": sorted(state["skipped_urls"]),
        "content_signatures": list(state["content_signatures"]),
        "word_counter": state["word_counter"],
        "subdomain_pages": {
            k: sorted(v) for k, v in state["subdomain_pages"].items()
        },
        "longest_url": state["longest_url"],
        "longest_count": state["longest_count"],
    }
    tmp = STATE_PATH + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, STATE_PATH)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def _drop_fragment(url):
    if not url:
        return ""
    base, _ = urldefrag(url)
    return base


def _split_into_tokens(text):
    return _TOKEN_RE.findall(text.lower())


def _parse_html(resp):
    try:
        return BeautifulSoup(resp.raw_response.content, features="html.parser")
    except (AttributeError, TypeError):
        return None


def _is_duplicate_content(resp, state):
    soup = _parse_html(resp)
    if soup is None:
        return False
    for tag in soup(["script", "style", "nav", "header", "footer", "noscript"]):
        tag.decompose()
    body_text = " ".join(soup.get_text(" ", strip=True).split())
    if not body_text:
        return False
    digest = md5(body_text.encode("utf-8")).hexdigest()
    if digest in state["content_signatures"]:
        return True
    state["content_signatures"].add(digest)
    return False


def extract_text_tokens(resp):
    soup = _parse_html(resp)
    if soup is None:
        return []
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return _split_into_tokens(soup.get_text(" ", strip=True))


def _tally_words(tokens, state):
    counter = state["word_counter"]
    for token in tokens:
        if len(token) > 2 and token not in STOPWORDS:
            counter[token] = counter.get(token, 0) + 1


def _update_longest(url, token_count, state):
    if token_count > state["longest_count"]:
        state["longest_url"] = url
        state["longest_count"] = token_count


def _record_subdomain(url, state):
    host = (urlparse(url).hostname or "").lower()
    if not host.endswith("uci.edu"):
        return
    if host.startswith("www."):
        host = host[4:]
    bucket = state["subdomain_pages"].setdefault(host, set())
    bucket.add(url)


# ---------------------------------------------------------------------------
# Crawler entry points
# ---------------------------------------------------------------------------

def scraper(url, resp):
    state = _load_state()

    if resp is None or resp.status != 200:
        state["skipped_urls"].add(url)
        _save_state(state)
        return []

    cleaned_url = _drop_fragment(url)
    if cleaned_url in state["crawled_urls"] or cleaned_url in state["skipped_urls"]:
        return []

    raw_links = extract_next_links(cleaned_url, resp)
    valid_links = [link for link in raw_links if is_valid(link)]

    if _is_duplicate_content(resp, state):
        state["skipped_urls"].add(cleaned_url)
        _save_state(state)
        return valid_links

    tokens = extract_text_tokens(resp)

    if len(tokens) >= MIN_TOKENS_FOR_PAGE:
        state["crawled_urls"].add(cleaned_url)
        _record_subdomain(cleaned_url, state)
        _update_longest(cleaned_url, len(tokens), state)
        _tally_words(tokens, state)
    else:
        state["skipped_urls"].add(cleaned_url)

    _save_state(state)
    if len(state["crawled_urls"]) % REPORT_FLUSH_EVERY == 0:
        write_report(state)

    return valid_links


def extract_next_links(url, resp):
    soup = _parse_html(resp)
    if soup is None:
        return []

    try:
        page_url = resp.raw_response.url or url
    except AttributeError:
        page_url = url

    next_links = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not href:
            continue
        if href.startswith(("javascript:", "mailto:", "tel:", "#",
                            "data:", "ftp:", "file:")):
            continue
        try:
            absolute = urljoin(page_url, href)
        except ValueError:
            continue
        absolute = _drop_fragment(absolute)
        if absolute:
            next_links.add(absolute)
    return next_links


def is_valid(url):
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except (ValueError, TypeError):
        return False

    if parsed.scheme not in {"http", "https"}:
        return False

    netloc = (parsed.netloc or "").lower()
    path = parsed.path or ""
    query = parsed.query or ""

    if not any(netloc == d or netloc.endswith("." + d)
               for d in ALLOWED_DOMAIN_SUFFIXES):
        return False

    if len(url) > MAX_URL_LEN:
        return False

    lowered_path = path.lower()
    if any(seg in lowered_path for seg in _DOWNLOAD_BUCKETS):
        return False

    if re.search(r"(?:^|&)(?:do|action)=(?!show\b|view\b)", query):
        return False

    target = f"{path}?{query}" if query else path
    if any(p.search(target) for p in _TRAP_PATTERNS):
        return False

    segments = [s for s in path.split("/") if s]
    if len(segments) > MAX_PATH_DEPTH:
        return False
    seen = {}
    for s in segments:
        seen[s] = seen.get(s, 0) + 1
        if seen[s] > MAX_REPEATED_SEGMENT:
            return False

    if _EXTENSION_BLACKLIST.match(lowered_path):
        return False

    return True


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_report(state=None):
    if state is None:
        state = _load_state()
    try:
        with open(REPORT_PATH, "w") as report:
            report.write(f"Unique Pages: {len(state['crawled_urls'])}\n\n\n")
            report.write(
                f"Longest Page: {state['longest_url']} "
                f"with {state['longest_count']} tokens\n\n\n"
            )
            report.write("Top 50 most frequent words:\n")
            ranked = sorted(
                state["word_counter"].items(),
                key=lambda kv: (-kv[1], kv[0]),
            )[:50]
            for word, freq in ranked:
                report.write(f"{word}: {freq}\n")
            report.write("\n\n\nSubdomain page counts:\n")
            for sub in sorted(state["subdomain_pages"]):
                report.write(f"{sub}, {len(state['subdomain_pages'][sub])}\n")
    except OSError:
        pass


if __name__ == "__main__":
    write_report()