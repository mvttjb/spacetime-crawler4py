from bs4 import BeautifulSoup
from collections import defaultdict, Counter
import json
import re
from urllib.parse import urljoin, urlparse, urldefrag


# Statistics for Report
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

    # Return empty if no response
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    links = []

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
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Do not crawl if it is not a valid domain
        domain = parsed.netloc.lower()
        allowed_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        if not any(domain.endswith(d) for d in allowed_domains):
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

def read_page(url, resp):

    global longest_page

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    text = soup.get_text(separator=" ", strip=True)

    # Cleanup and word splitting
    words = re.findall(r"[a-zA-Z]+", text.lower())
    word_count = len(words)

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
    global_word_counter.update(words)

def save_report(filename="crawler_report.json"):

    data = {
        "longest_page": longest_page,
        "num_unique_pages": len(page_word_counts),
        "unique_subdomains": len(subdomain_counter),
        "subdomain_counts": dict(subdomain_counter),
        "top_50_words": global_word_counter.most_common(50)
    }
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[SAVED] Report written to {filename}")
