import time
import requests
from types import SimpleNamespace
from urllib.parse import urljoin
from scraper import scraper, save_report, is_valid

# A simple local queue-based crawler (no ICS server dependency)
# Note: Have not tested beyond 1000 pages
def local_crawl(start_url, max_pages=20, delay=1.0):
    visited = set()
    frontier = [start_url]

    # Continue crawl for a set period of time (or until crawl ends)
    while frontier and len(visited) < max_pages:
        url = frontier.pop(0)
        if url in visited or not is_valid(url):
            continue

        # Begin crawl
        print(f"\n[Crawling] {url}")
        try:
            # Get response from url
            resp = requests.get(url, timeout=10)
            # Create a "response" from the url
            fake_resp = SimpleNamespace(
                status=resp.status_code,
                url=url,
                raw_response=SimpleNamespace(url=url, content=resp.content)
            )
            links = scraper(url, fake_resp)
            visited.add(url)

            # Add new valid links to the frontier
            for link in links:
                if link not in visited and is_valid(link):
                    frontier.append(link)

            # Print reports
            print(f"[Visited] {len(visited)} pages so far. Frontier: {len(frontier)}")
            time.sleep(delay)  # politeness delay

        except Exception as e:
            print(f"[Error] {url}: {e}")

    print("\nCrawl complete.")
    save_report("crawler_report.json")


if __name__ == "__main__":
    start = "https://www.ics.uci.edu/"
    local_crawl(start, max_pages=1000, delay=0.5)
