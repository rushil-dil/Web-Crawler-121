import re
import json
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup

MAX_CONTENT_SIZE = 5 * 1024 * 1024  # 5 MB

ALLOWED_DOMAINS = frozenset([
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
])

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

ANALYTICS_FILE = "analytics.json"
_analytics = None


def _load_analytics():
    global _analytics
    if _analytics is not None:
        return _analytics
    try:
        with open(ANALYTICS_FILE) as f:
            _analytics = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        _analytics = {
            "page_count": 0,
            "longest_page": {"url": "", "word_count": 0},
            "word_counts": {},
            "ics_subdomains": {},
        }
    return _analytics


def _save_analytics():
    with open(ANALYTICS_FILE, "w") as f:
        json.dump(_analytics, f)


def _update_analytics(url, words):
    data = _load_analytics()
    data["page_count"] += 1

    wc = len(words)
    if wc > data["longest_page"]["word_count"]:
        data["longest_page"] = {"url": url, "word_count": wc}

    counts = data["word_counts"]
    for word in words:
        w = word.lower()
        if len(w) >= 2 and w not in STOP_WORDS:
            counts[w] = counts.get(w, 0) + 1

    host = urlparse(url).hostname or ""
    if host.endswith(".ics.uci.edu"):
        data["ics_subdomains"][host] = data["ics_subdomains"].get(host, 0) + 1

    _save_analytics()


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # Non-200 covers all cache server error codes (600-608) too
    if resp.status != 200 or resp.raw_response is None:
        return []

    content = resp.raw_response.content
    if not content or len(content) > MAX_CONTENT_SIZE:
        return []

    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return []

    words = re.findall(r"[a-zA-Z]{2,}", soup.get_text())
    _update_analytics(url, words)

    base_url = resp.raw_response.url or url
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        defragged, _ = urldefrag(absolute)
        links.append(defragged)

    return links

def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        hostname = parsed.hostname
        if not hostname:
            return False

        # Only crawl within the four allowed domains and their subdomains
        if not any(hostname == d or hostname.endswith("." + d) for d in ALLOWED_DOMAINS):
            return False

        path_parts = [p for p in parsed.path.split("/") if p]

        # Repeating path segments signal a loop trap (e.g. /a/b/a/)
        if len(path_parts) != len(set(path_parts)):
            return False

        # Excessively deep paths are almost always traps
        if len(path_parts) > 10:
            return False

        # Long query strings create near-infinite URL spaces
        if len(parsed.query) > 200:
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
