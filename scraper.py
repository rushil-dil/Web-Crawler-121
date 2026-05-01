import re
from hashlib import md5
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict


longest_record = ('', 0)
word_counter = defaultdict(int)
subdomain_pages = defaultdict(set)
skipped_urls = set()
crawled_urls = set()
content_signatures = set()

stopwords = {
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
    "yours", "yourself", "yourselves"
}

ALLOWED_DOMAIN_SUFFIXES = (
    'ics.uci.edu',
    'cs.uci.edu',
    'informatics.uci.edu',
    'stat.uci.edu',
)

MIN_TOKENS_FOR_PAGE = 150
MAX_URL_LEN = 280
REPORT_FLUSH_EVERY = 30
_pages_since_flush = 0


def scraper(url, resp):
    global _pages_since_flush

    if resp is None or resp.status != 200:
        skipped_urls.add(url)
        return []

    cleaned_url = _drop_fragment(url)
    if cleaned_url in crawled_urls or cleaned_url in skipped_urls:
        return []

    if _is_duplicate_content(resp):
        skipped_urls.add(cleaned_url)
        return []

    token_list = extract_text_tokens(resp)
    if len(token_list) < MIN_TOKENS_FOR_PAGE:
        skipped_urls.add(cleaned_url)
        return []

    crawled_urls.add(cleaned_url)
    _record_subdomain(cleaned_url)
    _update_longest(cleaned_url, len(token_list))
    _tally_words(token_list)

    _pages_since_flush += 1
    if _pages_since_flush >= REPORT_FLUSH_EVERY:
        write_report()
        _pages_since_flush = 0

    raw_links = extract_next_links(cleaned_url, resp)
    return [link for link in raw_links if is_valid(link)]


def extract_next_links(url, resp):
    next_links = set()
    try:
        page_url = resp.raw_response.url or url
    except AttributeError:
        page_url = url

    try:
        soup = BeautifulSoup(resp.raw_response.content, features='html.parser')
    except (AttributeError, TypeError):
        return []

    for anchor in soup.find_all('a', href=True):
        href = anchor['href'].strip()
        if not href:
            continue
        if href.startswith(('javascript:', 'mailto:', 'tel:', '#',
                            'data:', 'ftp:', 'file:')):
            continue

        try:
            absolute = urljoin(page_url, href)
        except ValueError:
            continue

        absolute = _drop_fragment(absolute)
        if absolute:
            next_links.add(absolute)

    return next_links


def _is_duplicate_content(resp):
    """Hash the visible body text; skip pages we've already ingested verbatim."""
    try:
        soup = BeautifulSoup(resp.raw_response.content, features='html.parser')
    except (AttributeError, TypeError):
        return False

    for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript']):
        tag.decompose()

    body_text = ' '.join(soup.get_text(' ', strip=True).split())
    if not body_text:
        return False

    digest = md5(body_text.encode('utf-8')).hexdigest()
    if digest in content_signatures:
        return True
    content_signatures.add(digest)
    return False


def extract_text_tokens(resp):
    """Pull the page body and break it into normalised word tokens."""
    try:
        soup = BeautifulSoup(resp.raw_response.content, features='html.parser')
    except (AttributeError, TypeError):
        return []

    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()

    return _split_into_tokens(soup.get_text(' ', strip=True))


def _split_into_tokens(text):
    """Letters-only tokens (length >= 2). Numbers don't count as words."""
    return re.findall(r"[a-z][a-z']+", text.lower())


def _tally_words(token_list):
    for token in token_list:
        if len(token) > 2 and token not in stopwords:
            word_counter[token] += 1


def _update_longest(url, token_count):
    global longest_record
    if token_count > longest_record[1]:
        longest_record = (url, token_count)


def _record_subdomain(url):
    """Track unique pages per uci.edu subdomain (set, not counter)."""
    host = (urlparse(url).hostname or '').lower()
    if not host.endswith('uci.edu'):
        return
    if host.startswith('www.'):
        host = host[4:]
    subdomain_pages[host].add(url)


def _drop_fragment(url):
    if not url:
        return ''
    base, _ = urldefrag(url)
    return base


def write_report():
    """Persist analytics to a text file so a restart doesn't lose them."""
    try:
        with open('crawler_report.txt', 'w') as report:
            report.write(f"Unique Pages: {len(crawled_urls)}\n\n\n")

            report.write(
                f"Longest Page: {longest_record[0]} "
                f"with {longest_record[1]} tokens\n\n\n"
            )

            report.write("Top 50 most frequent words:\n")
            ranked = sorted(
                word_counter.items(),
                key=lambda kv: (-kv[1], kv[0]),
            )[:50]
            for word, freq in ranked:
                report.write(f"{word}: {freq}\n")

            report.write("\n\n\nSubdomain page counts:\n")
            for sub in sorted(subdomain_pages):
                report.write(f"{sub}, {len(subdomain_pages[sub])}\n")
    except OSError:
        pass


def is_valid(url):
    if not url:
        return False
    if url in crawled_urls or url in skipped_urls:
        return False

    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        netloc = (parsed.netloc or '').lower()
        path = parsed.path or ''
        query = parsed.query or ''

        if not any(netloc == d or netloc.endswith('.' + d)
                   for d in ALLOWED_DOMAIN_SUFFIXES):
            return False

        if len(url) > MAX_URL_LEN:
            return False

        # Buckets that almost always hold downloads / templated noise.
        if any(seg in path.lower() for seg in
               ('/files/', '/sampledata/', '/supplement/',
                '/wp-content/uploads/', '/raw-attachment/')):
            return False

        # dokuwiki / mediawiki action endpoints (edit, history, diff, ...).
        if re.search(r'(?:^|&)(?:do|action)=(?!show\b|view\b)', query):
            return False

        # Calendar / event traps + dated permalink farms.
        trap_signatures = (
            r'/calendar(?:/|$)',
            r'/events?/.*\d{4}',
            r'/\d{4}/\d{2}/\d{2}(?:/|$)',
            r'/\d{4}-\d{2}-\d{2}(?:/|$)',
            r'tribe-bar-date=',
            r'eventDate=',
            r'(?:^|&)ical=',
            r'/page/\d{3,}(?:/|$)',
            r'/feed/?$',
            r'/(?:atom|rss)/?$',
        )
        target = f"{path}?{query}" if query else path
        if any(re.search(sig, target, re.IGNORECASE) for sig in trap_signatures):
            return False

        # Repeated path segments (e.g. /a/b/a/b/a/b/...).
        segments = [s for s in path.split('/') if s]
        if len(segments) > 12:
            return False
        seen = {}
        for s in segments:
            seen[s] = seen.get(s, 0) + 1
            if seen[s] > 2:
                return False

        # File-extension blacklist.
        return not re.match(
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
            r"|rm|smil|wmv|swf|wma|zip|rar|gz|z|lz|xz)$",
            path.lower(),
        )

    except TypeError:
        return False
