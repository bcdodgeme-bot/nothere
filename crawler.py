"""
NotHere.one Web Crawler
Production-ready crawler with Tier 1 filtering
"""

import os
import sys
import time
import argparse
import logging
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from datetime import datetime
import hashlib

import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values

from blocklist import get_blocklist


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Crawler:
    """
    Web crawler with built-in Tier 1 filtering
    """
    
    def __init__(self, redis_manager, db_conn, politeness_delay=1.0):
        self.redis = redis_manager
        self.db_conn = db_conn
        self.blocklist = get_blocklist()
        self.politeness_delay = politeness_delay
        self.robots_cache = {}  # Cache robots.txt parsers
        
        # User agent
        self.headers = {
            'User-Agent': 'NotHere.one Bot/1.0 (Values-based search engine; +https://nothere.one/bot)'
        }
        
        # Statistics
        self.stats = {
            'pages_crawled': 0,
            'pages_blocked': 0,
            'pages_failed': 0,
            'links_found': 0,
            'urls_queued': 0
        }
    
    def normalize_url(self, url):
        """Normalize URL for consistent handling"""
        url = url.strip()
        
        # Remove fragment
        if '#' in url:
            url = url[:url.index('#')]
        
        # Ensure scheme
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url
    
    def get_url_hash(self, url):
        """Generate hash for URL (for deduplication)"""
        return hashlib.sha256(url.encode()).hexdigest()
    
    def is_url_crawled(self, url_hash):
        """Check if URL has already been crawled"""
        cursor = self.db_conn.cursor()
        cursor.execute(
            "SELECT 1 FROM pages WHERE url_hash = %s LIMIT 1",
            (url_hash,)
        )
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    
    def can_fetch(self, url):
        """Check robots.txt to see if we can fetch this URL"""
        try:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            # Check cache
            if base_url not in self.robots_cache:
                robots_url = urljoin(base_url, '/robots.txt')
                rp = RobotFileParser()
                rp.set_url(robots_url)
                
                try:
                    rp.read()
                    self.robots_cache[base_url] = rp
                except Exception as e:
                    logger.debug(f"Could not read robots.txt for {base_url}: {e}")
                    # If robots.txt doesn't exist or can't be read, allow crawling
                    self.robots_cache[base_url] = None
            
            rp = self.robots_cache[base_url]
            if rp is None:
                return True
            
            return rp.can_fetch(self.headers['User-Agent'], url)
            
        except Exception as e:
            logger.error(f"Error checking robots.txt for {url}: {e}")
            # Default to allowing if check fails
            return True
    
    def fetch_page(self, url):
        """Fetch page content"""
        try:
            response = requests.get(
                url,
                headers=self.headers,
                timeout=10,
                allow_redirects=True
            )
            
            # Only process successful responses with HTML content
            if response.status_code != 200:
                logger.warning(f"Non-200 status {response.status_code} for {url}")
                return None
            
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' not in content_type:
                logger.debug(f"Skipping non-HTML content for {url}: {content_type}")
                return None
            
            return response
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_content(self, html, base_url):
        """Extract title, text content, and links from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(['script', 'style', 'nav', 'footer', 'header']):
                script.decompose()
            
            # Extract title
            title = None
            if soup.title:
                title = soup.title.string.strip() if soup.title.string else None
            
            # Extract main text content
            text_content = soup.get_text(separator=' ', strip=True)
            
            # Clean up whitespace
            text_content = ' '.join(text_content.split())
            
            # Extract links
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Skip non-http(s) links
                if href.startswith(('mailto:', 'tel:', 'javascript:')):
                    continue
                
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, href)
                
                # Normalize
                absolute_url = self.normalize_url(absolute_url)
                
                # Get link text
                link_text = link.get_text(strip=True)
                
                links.append({
                    'url': absolute_url,
                    'text': link_text[:500] if link_text else None  # Limit link text length
                })
            
            return {
                'title': title[:500] if title else None,  # Limit title length
                'content': text_content[:50000],  # Limit content length
                'links': links
            }
            
        except Exception as e:
            logger.error(f"Error extracting content: {e}")
            return None
    
    def save_page(self, url, url_hash, title, content, crawled_at):
        """Save page to database"""
        cursor = self.db_conn.cursor()
        
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            
            cursor.execute("""
                INSERT INTO pages (url, url_hash, domain, title, content, crawled_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url_hash) 
                DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    crawled_at = EXCLUDED.crawled_at
                RETURNING id
            """, (url, url_hash, domain, title, content, crawled_at, crawled_at))
            
            page_id = cursor.fetchone()[0]
            self.db_conn.commit()
            
            return page_id
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error saving page {url}: {e}")
            return None
        finally:
            cursor.close()
    
    def save_links(self, source_page_id, links):
        """Save links to database"""
        if not links:
            return
        
        cursor = self.db_conn.cursor()
        
        try:
            # Prepare data for batch insert
            link_data = [
                (source_page_id, link['url'], link['text'])
                for link in links
            ]
            
            execute_values(
                cursor,
                """
                INSERT INTO links (source_page_id, target_url, link_text)
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                link_data
            )
            
            self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error saving links: {e}")
        finally:
            cursor.close()
    
    def queue_url(self, url):
        """Add URL to crawl queue if not blocked and not already crawled"""
        # Check blocklist
        is_blocked, reason = self.blocklist.is_blocked(url)
        if is_blocked:
            logger.debug(f"Blocked URL (not queuing): {url} - {reason}")
            return False
        
        # Check if already crawled
        url_hash = self.get_url_hash(url)
        if self.is_url_crawled(url_hash):
            logger.debug(f"Already crawled (not queuing): {url}")
            return False
        
        # Add to queue
        self.redis.enqueue_url(url)
        self.stats['urls_queued'] += 1
        return True
    
    def crawl_url(self, url):
        """Crawl a single URL"""
        logger.info(f"Crawling: {url}")
        
        # Normalize URL
        url = self.normalize_url(url)
        url_hash = self.get_url_hash(url)
        
        # Check if already crawled
        if self.is_url_crawled(url_hash):
            logger.info(f"Already crawled: {url}")
            return
        
        # Check Tier 1 blocklist
        is_blocked, reason = self.blocklist.is_blocked(url)
        if is_blocked:
            logger.warning(f"⛔ Blocked: {url} - {reason}")
            self.stats['pages_blocked'] += 1
            return
        
        # Check robots.txt
        if not self.can_fetch(url):
            logger.warning(f"🤖 Disallowed by robots.txt: {url}")
            self.stats['pages_blocked'] += 1
            return
        
        # Politeness delay
        time.sleep(self.politeness_delay)
        
        # Fetch page
        response = self.fetch_page(url)
        if response is None:
            self.stats['pages_failed'] += 1
            return
        
        # Get final URL after redirects
        final_url = response.url
        final_url_hash = self.get_url_hash(final_url)
        
        # Check if redirected URL is blocked
        if final_url != url:
            is_blocked, reason = self.blocklist.is_blocked(final_url)
            if is_blocked:
                logger.warning(f"⛔ Blocked after redirect: {final_url} - {reason}")
                self.stats['pages_blocked'] += 1
                return
        
        # Extract content
        extracted = self.extract_content(response.text, final_url)
        if extracted is None:
            self.stats['pages_failed'] += 1
            return
        
        # Save page
        crawled_at = datetime.utcnow()
        page_id = self.save_page(
            final_url,
            final_url_hash,
            extracted['title'],
            extracted['content'],
            crawled_at
        )
        
        if page_id is None:
            self.stats['pages_failed'] += 1
            return
        
        # Save links
        self.save_links(page_id, extracted['links'])
        
        # Queue new URLs
        for link in extracted['links']:
            self.queue_url(link['url'])
        
        # Update stats
        self.stats['pages_crawled'] += 1
        self.stats['links_found'] += len(extracted['links'])
        
        logger.info(f"✅ Crawled successfully: {final_url} ({len(extracted['links'])} links found)")
    
    def crawl(self, max_pages=None):
        """Main crawl loop"""
        logger.info(f"Starting crawler (max_pages={max_pages})")
        
        pages_crawled = 0
        
        try:
            while True:
                # Check if we've hit the limit
                if max_pages and pages_crawled >= max_pages:
                    logger.info(f"Reached max_pages limit: {max_pages}")
                    break
                
                # Get URL from queue
                url = self.redis.dequeue_url()
                
                if url is None:
                    logger.info("Queue is empty")
                    break
                
                # Crawl the URL
                self.crawl_url(url)
                pages_crawled += 1
                
                # Print progress every 10 pages
                if pages_crawled % 10 == 0:
                    self.print_stats()
        
        except KeyboardInterrupt:
            logger.info("\nCrawl interrupted by user")
        
        finally:
            self.print_stats()
            logger.info("Crawler stopped")
    
    def print_stats(self):
        """Print crawl statistics"""
        logger.info("\n" + "="*60)
        logger.info("CRAWL STATISTICS")
        logger.info("="*60)
        logger.info(f"Pages crawled:     {self.stats['pages_crawled']}")
        logger.info(f"Pages blocked:     {self.stats['pages_blocked']}")
        logger.info(f"Pages failed:      {self.stats['pages_failed']}")
        logger.info(f"Links found:       {self.stats['links_found']}")
        logger.info(f"URLs queued:       {self.stats['urls_queued']}")
        logger.info("="*60 + "\n")


def load_seed_urls(seed_file):
    """Load seed URLs from file"""
    urls = []
    with open(seed_file, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


def main():
    parser = argparse.ArgumentParser(description='NotHere.one Web Crawler')
    parser.add_argument('--seed', type=str, help='Seed URLs file')
    parser.add_argument('--max-pages', type=int, help='Maximum pages to crawl')
    parser.add_argument('--delay', type=float, default=1.0, help='Politeness delay in seconds (default: 1.0)')
    
    args = parser.parse_args()
    
    # Get database connection
    try:
        db_conn = psycopg2.connect(os.environ['DATABASE_URL'])
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)
    
    # Import RedisManager (assuming it's in the same directory or in PYTHONPATH)
    try:
        from redis_manager import RedisManager
        redis_manager = RedisManager()
        logger.info("✅ Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Load seed URLs if provided
    if args.seed:
        try:
            seed_urls = load_seed_urls(args.seed)
            logger.info(f"Loading {len(seed_urls)} seed URLs...")
            
            for url in seed_urls:
                redis_manager.enqueue_url(url)
            
            logger.info(f"✅ Queued {len(seed_urls)} seed URLs")
        except Exception as e:
            logger.error(f"Failed to load seed URLs: {e}")
            sys.exit(1)
    
    # Auto-seed if queue is empty and no seed file provided
    if not args.seed and redis_manager.queue_size() == 0:
        logger.info("Queue is empty, loading default seeds...")
        try:
            seed_urls = load_seed_urls('seed_urls.txt')
            for url in seed_urls:
                redis_manager.enqueue_url(url)
            logger.info(f"✅ Auto-seeded {len(seed_urls)} URLs")
        except Exception as e:
            logger.warning(f"Could not auto-seed URLs: {e}")
    
    # Create crawler
    # Auto-seed if queue is empty and no seed file provided
    if not args.seed and redis_manager.queue_size() == 0:
        logger.info("Queue is empty, loading default seeds...")
        try:
            seed_urls = load_seed_urls('seed_urls.txt')
            for url in seed_urls:
                redis_manager.enqueue_url(url)
            logger.info(f"✅ Auto-seeded {len(seed_urls)} URLs")
        except Exception as e:
            logger.warning(f"Could not auto-seed URLs: {e}")
    
    crawler = Crawler(
        redis_manager=redis_manager,
        db_conn=db_conn,
        politeness_delay=args.delay
    )
    
    # Print blocklist stats
    stats = crawler.blocklist.get_stats()
    logger.info(f"Tier 1 Blocklist loaded: {stats['blocked_domains']} domains, "
                f"{stats['blocked_tlds']} TLDs, {stats['blocked_patterns']} patterns")
    
    # Start crawling
    crawler.crawl(max_pages=args.max_pages)
    
    # Cleanup
    db_conn.close()
    logger.info("Database connection closed")


if __name__ == '__main__':
    main()
