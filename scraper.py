from bs4 import BeautifulSoup
from collections import defaultdict, Counter
import json
import re
from urllib.parse import urljoin, urlparse, urldefrag


# Statistics for Report (Using collections library)
page_word_counts = {}
subdomain_counter = defaultdict(int)
longest_page = ("", 0)
global_word_counter = Counter()


def scraper(url, resp):
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]

    if resp.status == 200 and resp.raw_response:
        read_page(url, resp)

    return valid_links

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

    # Skip dead or empty responses
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    # Skip pages that are too large
    if len(resp.raw_response.content) > 10000000:
        return []
    
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    links = []

    # Skip pages that are too small
    if len(text.split()) < 100:
        return []

    # Extract next links
    for tag in soup.find_all("a", href=True):
        href = tag['href']
        abs_url = urljoin(resp.url, href)       # Resolve relative URLs
        abs_url, _ = urldefrag(abs_url)         # Remove fragments (e.g. #section)
        links.append(abs_url)

    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)

        # Only allow HTTP/HTTPS schemes
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Restrict crawling to allowed domains
        domain = parsed.netloc.lower()
        allowed_domains = [
            "ics.uci.edu", 
            "cs.uci.edu", 
            "informatics.uci.edu", 
            "stat.uci.edu"
        ]
        if not any(domain.endswith(d) for d in allowed_domains):
            return False
        
        # Observable crawler traps / avoidances
        trap_patterns = [
            "wics.ics.uci.edu",
            "ngs.ics.uci.edu",
            "?ical", "tribe", "calendar",
            "~eppstein/pix",
            "isg.ics.uci.edu/events",
            "doku.php", "grape",
            "fano.ics.uci.edu/ca/rules/",
            "?filter"
        ]
        if any(t in url.lower() for t in trap_patterns):
            return False
        
        # Avoid calendar/date loops or page loops (Through Regex)
        if re.search(r"(\?|&)page=\d+", url):
            return False
        if re.search(r"(\?|&)month=\d+", url):
            return False

        # File type filters (Skipping Non-HTML Pages)
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

    except ValueError:
        print ("ValueError for ", parsed)
        raise

def read_page(url, resp):
    # Helper function for analyzing valid pages
    # Uses BeautifulSoup package for simplicity
    # Updates statistics for report

    global longest_page

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    text = soup.get_text(separator=" ", strip=True)

    # Cleanup and word splitting
    words = re.findall(r"[a-zA-Z]+", text.lower())
    word_count = len(words)

    # Skip empty or nearly empty pages
    if word_count < 100:
        return

    # Update per-page word count
    page_word_counts[url] = word_count

    # Track subdomain frequency
    parsed = urlparse(url)
    subdomain = parsed.netloc.lower()
    subdomain_counter[subdomain] += 1

    # Track longest page
    if word_count > longest_page[1]:
        longest_page = (url, word_count)

    # Update global word counter for most common words
    # Ignore all stop words!
    STOP_WORDS = {
        "a","about","above","after","again","against","all","am","an","and","any", "are","as","at","be","because","been","before","being","below","between",
        "both","but","by","could","d","did","do","does","doing","down","during","each", "few","for","from","further","had","has","have","having","he","her","here","hers","herself","him","himself","his","how","if","in","into","is", "it","its","itself","just","me","more","most","my","myself","no","nor", "not","now","of","off",
        "on","once","only","or","other","our","ours","ourselves","out","over","own","s","same","she","should","so","some","such", "than","that","the","their","theirs","them","themselves","then","there","these","they","this","those","through","to","too","under","until","up","very","was","we","were","what","when","where","which","while","who",
        "whom","why","with","would","you","your","yours","yourself","yourselves","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x",
        "y","z"
    }
    global_word_counter.update(w for w in words if w not in STOP_WORDS)

    # Save report at the end of reading page
    # Only after every 50 pages to minimize disk I/O
    if len(page_word_counts) % 50 == 0:
        save_report("crawler_report.json")

def save_report(filename="crawler_report.json"):

    # Call when wanting to save a new report
    # Saves into a new / existing JSON file

    data = {
        "longest_page": longest_page,
        "num_unique_pages": len(page_word_counts),
        "unique_subdomains": len(subdomain_counter),
        "subdomain_counts": dict(subdomain_counter),
        "top_50_words": global_word_counter.most_common(50)
    }
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
