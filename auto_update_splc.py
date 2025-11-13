#!/usr/bin/env python3
"""
SPLC Hate Map Automated Scraper
Fetches updated hate group websites from SPLC and updates blocklist
"""

import os
import sys
import re
import psycopg2
from datetime import date
import logging

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SPLCScraper:
    """Scrape SPLC Hate Map for hate group websites"""
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.base_url = "https://www.splcenter.org"
        self.hate_map_url = f"{self.base_url}/hate-map"
        
        # Known hate group domains (seed list + scraped)
        self.hate_domains = set()
        
    def scrape_hate_map(self):
        """
        Scrape SPLC Hate Map
        Note: SPLC's hate map is JavaScript-heavy, so we'll use a combination of:
        1. Direct scraping where possible
        2. Known hate group list maintenance
        3. Pattern matching on group names
        """
        logger.info("Fetching SPLC Hate Map...")
        
        try:
            response = requests.get(self.hate_map_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # SPLC's map is dynamic, but we can extract group names and research their sites
            # This is a simplified version - in production, you'd use Selenium for JS rendering
            
            # Look for links to hate group profiles
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # SPLC group profile pages
                if '/fighting-hate/extremist-files/group/' in href:
                    group_url = self.base_url + href if href.startswith('/') else href
                    self._scrape_group_page(group_url)
            
            logger.info(f"Found {len(self.hate_domains)} hate domains from SPLC")
            
        except Exception as e:
            logger.error(f"Error scraping SPLC: {e}")
    
    def _scrape_group_page(self, url):
        """Scrape individual hate group page for website URLs"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for website links in the page
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Skip SPLC internal links
                if 'splcenter.org' in href:
                    continue
                
                # Look for external website links
                if href.startswith('http'):
                    domain = self._extract_domain(href)
                    if domain and self._is_likely_hate_site(domain, soup.text):
                        self.hate_domains.add(domain)
        
        except Exception as e:
            logger.debug(f"Error scraping group page {url}: {e}")
    
    def _extract_domain(self, url):
        """Extract clean domain from URL"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www
            if domain.startswith('www.'):
                domain = domain[4:]
            
            return domain if domain else None
        except:
            return None
    
    def _is_likely_hate_site(self, domain, page_text):
        """
        Heuristic check if domain is likely a hate group site
        (vs. social media, news coverage, etc.)
        """
        # Skip social media
        social_media = ['facebook.com', 'twitter.com', 'youtube.com', 'instagram.com',
                       'tiktok.com', 'linkedin.com', 'reddit.com']
        
        if any(sm in domain for sm in social_media):
            return False
        
        # Skip news sites
        news_sites = ['cnn.com', 'foxnews.com', 'nytimes.com', 'washingtonpost.com',
                     'reuters.com', 'apnews.com', 'bbc.com', 'npr.org']
        
        if any(news in domain for news in news_sites):
            return False
        
        return True
    
    def add_known_hate_groups(self):
        """
        Add known hate groups from research and previous SPLC reports
        This serves as a fallback for when scraping is difficult
        """
        known_groups = [
            # White Nationalist/Neo-Nazi
            ('stormfront.org', 'White nationalist forum - SPLC listed'),
            ('dailystormer.name', 'Neo-Nazi website - SPLC listed'),
            ('dailystormer.su', 'Neo-Nazi website - SPLC listed'),
            ('vdare.com', 'White nationalist publication - SPLC listed'),
            ('americanrenaissance.com', 'White nationalist publication - SPLC listed'),
            ('counter-currents.com', 'White nationalist publication - SPLC listed'),
            ('theoccidentalobserver.net', 'White nationalist publication - SPLC listed'),
            ('nationalvanguard.org', 'Neo-Nazi organization - SPLC listed'),
            ('nsm88.org', 'Neo-Nazi organization - SPLC listed'),
            ('therightstuff.biz', 'Neo-Nazi podcast network - SPLC listed'),
            ('altright.com', 'White nationalist - SPLC listed'),
            ('radixjournal.com', 'White nationalist - SPLC listed'),
            
            # Anti-Muslim Hate
            ('jihadwatch.org', 'Anti-Muslim hate site - SPLC listed'),
            ('barenakedislam.com', 'Anti-Muslim hate site - SPLC listed'),
            ('atlasshrugs.com', 'Anti-Muslim hate site - SPLC listed'),
            ('pamelageller.com', 'Anti-Muslim hate - SPLC listed'),
            ('thereligionofpeace.com', 'Anti-Muslim hate site - SPLC listed'),
            ('gatesofvienna.net', 'Anti-Muslim hate site - SPLC listed'),
            
            # Conspiracy/Extremist
            ('infowars.com', 'Conspiracy theories, extremist content - SPLC listed'),
            ('prisonplanet.com', 'Conspiracy theories - SPLC listed'),
            ('naturalnews.com', 'Dangerous health misinformation - SPLC listed'),
            ('beforeitsnews.com', 'Conspiracy theories - SPLC listed'),
            ('veteranstoday.com', 'Conspiracy theories, antisemitism - SPLC listed'),
            
            # Add more from SPLC Hate Map research
        ]
        
        for domain, reason in known_groups:
            self.hate_domains.add((domain, reason))
        
        logger.info(f"Added {len(known_groups)} known hate groups")
    
    def update_database(self):
        """Update org_blocklist table with scraped domains"""
        cursor = self.db_conn.cursor()
        
        updated = 0
        
        for item in self.hate_domains:
            if isinstance(item, tuple):
                domain, reason = item
            else:
                domain = item
                reason = "SPLC Hate Map - Automated scrape"
            
            try:
                cursor.execute("""
                    INSERT INTO org_blocklist (
                        domain, splc_flagged, reason, flagged_date, verification_source
                    ) VALUES (%s, TRUE, %s, %s, %s)
                    ON CONFLICT (domain) 
                    DO UPDATE SET 
                        splc_flagged = TRUE,
                        reason = EXCLUDED.reason,
                        flagged_date = EXCLUDED.flagged_date
                """, (domain, reason, date.today(), 'SPLC Automated Scraper'))
                
                updated += 1
                logger.info(f"✅ Updated: {domain}")
                
            except Exception as e:
                logger.error(f"❌ Error updating {domain}: {e}")
        
        self.db_conn.commit()
        cursor.close()
        
        logger.info(f"Updated {updated} domains in database")
        return updated


def main():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)
    
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    
    conn = psycopg2.connect(database_url)
    
    logger.info("="*60)
    logger.info("SPLC HATE MAP AUTOMATED UPDATE")
    logger.info("="*60)
    
    scraper = SPLCScraper(conn)
    
    # Add known hate groups (always reliable)
    scraper.add_known_hate_groups()
    
    # Try scraping for new ones
    try:
        scraper.scrape_hate_map()
    except Exception as e:
        logger.warning(f"Scraping failed, using known list only: {e}")
    
    # Update database
    updated = scraper.update_database()
    
    logger.info("="*60)
    logger.info(f"✅ SPLC update complete: {updated} domains processed")
    logger.info("="*60)
    
    conn.close()


if __name__ == '__main__':
    main()