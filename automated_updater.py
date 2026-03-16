#!/usr/bin/env python3
"""
Automated Scoring Data Updater
Master scheduler that runs all automated scrapers and updates
"""

import os
import sys
import logging
from datetime import datetime
import psycopg2

# Import our scrapers
from auto_update_splc import SPLCScraper
from auto_update_bcorp import BCorpScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AutomatedUpdater:
    """Run all automated data updates"""
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.results = {
            'splc': {'success': False, 'count': 0, 'error': None},
            'bcorp': {'success': False, 'count': 0, 'error': None},
        }
    
    def update_blocklists(self):
        """Update all blocklist sources"""
        logger.info("🛡️  Updating blocklists...")
        logger.warning("NOTE: SPLC and B-Corp updates use separate DB operations. "
                       "If B-Corp update fails after SPLC succeeds, partial state may exist.")
        
        # SPLC
        try:
            logger.info("Running SPLC scraper...")
            scraper = SPLCScraper(self.db_conn)
            scraper.add_known_hate_groups()
            
            try:
                scraper.scrape_hate_map()
            except Exception as e:
                logger.warning(f"SPLC scraping failed (using known list): {e}")
            
            count = scraper.update_database()
            self.results['splc'] = {'success': True, 'count': count, 'error': None}
            
        except Exception as e:
            logger.error(f"SPLC update failed: {e}")
            self.results['splc'] = {'success': False, 'count': 0, 'error': str(e)}
    
    def update_equity_domains(self):
        """Update all equity domain sources"""
        logger.info("✊ Updating equity domains...")
        
        # B-Corp
        try:
            logger.info("Running B-Corp scraper...")
            scraper = BCorpScraper(self.db_conn)
            scraper.scrape_directory()
            count = scraper.update_database()
            self.results['bcorp'] = {'success': True, 'count': count, 'error': None}
            
        except Exception as e:
            logger.error(f"B-Corp update failed: {e}")
            self.results['bcorp'] = {'success': False, 'count': 0, 'error': str(e)}
    
    def rescore_affected_pages(self):
        """
        Rescore pages that might be affected by updates
        For now, just log a note - in production you'd want selective rescoring
        """
        logger.info("📊 Checking for pages needing rescoring...")
        
        cursor = self.db_conn.cursor()

        try:
            # Count pages from updated domains
            cursor.execute("""
                SELECT COUNT(*)
                FROM pages p
                WHERE EXISTS (
                    SELECT 1 FROM org_blocklist ob
                    WHERE p.domain = ob.domain
                )
            """)

            blocklisted_pages = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*)
                FROM pages p
                WHERE EXISTS (
                    SELECT 1 FROM equity_domains ed
                    WHERE p.domain = ed.domain
                )
            """)

            equity_pages = cursor.fetchone()[0]
        finally:
            cursor.close()
        
        logger.info(f"  • {blocklisted_pages} pages from blocklisted domains")
        logger.info(f"  • {equity_pages} pages from equity domains")
        
        if blocklisted_pages > 0 or equity_pages > 0:
            logger.info("  ⚠️  Consider running rescore for affected domains")
            # TODO: Implement selective rescoring
            # For now, next crawl will pick up new scores
    
    def send_notification(self):
        """
        Send notification about update results
        Could be email, Slack, etc.
        """
        success_count = sum(1 for r in self.results.values() if r['success'])
        total_count = len(self.results)
        
        if success_count == total_count:
            logger.info(f"✅ All updates successful ({success_count}/{total_count})")
        else:
            logger.warning(f"⚠️  Some updates failed ({success_count}/{total_count} successful)")
            
            for source, result in self.results.items():
                if not result['success']:
                    logger.error(f"  • {source}: {result['error']}")
    
    def generate_report(self):
        """Generate update report"""
        logger.info("\n" + "="*60)
        logger.info("UPDATE REPORT")
        logger.info("="*60)
        logger.info(f"Run time: {datetime.now().isoformat()}")
        logger.info("")
        
        for source, result in self.results.items():
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            logger.info(f"{source.upper()}: {status}")
            
            if result['success']:
                logger.info(f"  • Updated {result['count']} domains")
            else:
                logger.info(f"  • Error: {result['error']}")
        
        logger.info("="*60)


def main():
    logger.info("="*60)
    logger.info("AUTOMATED DATA UPDATER - STARTED")
    logger.info("="*60)
    
    # Get database connection
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)
    
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    
    try:
        conn = psycopg2.connect(database_url)
        logger.info("✅ Connected to database")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        sys.exit(1)
    
    # Run updates
    updater = AutomatedUpdater(conn)
    
    try:
        # Update blocklists
        updater.update_blocklists()
        
        # Update equity domains
        updater.update_equity_domains()
        
        # Check for pages needing rescoring
        updater.rescore_affected_pages()
        
        # Send notification
        updater.send_notification()
        
        # Generate report
        updater.generate_report()
        
    except Exception as e:
        logger.error(f"❌ Updater failed: {e}")
        raise
    
    finally:
        conn.close()
        logger.info("Database connection closed")
    
    logger.info("="*60)
    logger.info("AUTOMATED DATA UPDATER - COMPLETED")
    logger.info("="*60)


if __name__ == '__main__':
    main()