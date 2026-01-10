#!/usr/bin/env python3
"""
Test script for NotHere.one crawler components
Verifies blocklist, Redis, and database connections
"""

import os
import sys
import psycopg2
from blocklist import get_blocklist


def test_blocklist():
    """Test the Tier 1 blocklist"""
    print("\n" + "="*60)
    print("TESTING BLOCKLIST")
    print("="*60)
    
    blocklist = get_blocklist()
    stats = blocklist.get_stats()
    
    print(f"✓ Blocklist loaded")
    print(f"  - {stats['blocked_domains']} blocked domains")
    print(f"  - {stats['blocked_tlds']} blocked TLDs")
    print(f"  - {stats['blocked_patterns']} blocked patterns")
    
    # Test cases
    test_cases = [
        ('https://wikipedia.org', False, 'Safe educational site'),
        ('https://bbc.com', False, 'Safe news site'),
        ('https://pornhub.com', True, 'Adult content'),
        ('https://example.com/casino/', True, 'Gambling URL pattern'),
        ('https://test.xxx', True, 'Adult TLD'),
        ('https://infowars.com', True, 'Misinformation site'),
    ]
    
    print("\nTest cases:")
    all_passed = True
    for url, should_block, description in test_cases:
        is_blocked, reason = blocklist.is_blocked(url)
        
        if is_blocked == should_block:
            status = "✓ PASS"
        else:
            status = "✗ FAIL"
            all_passed = False
        
        print(f"  {status}: {description}")
        print(f"    URL: {url}")
        print(f"    Expected: {'BLOCK' if should_block else 'ALLOW'}, Got: {'BLOCK' if is_blocked else 'ALLOW'}")
        if is_blocked:
            print(f"    Reason: {reason}")
        print()
    
    if all_passed:
        print("✅ All blocklist tests passed!\n")
        return True
    else:
        print("❌ Some blocklist tests failed!\n")
        return False


def test_redis():
    """Test Redis connection"""
    print("="*60)
    print("TESTING REDIS")
    print("="*60)
    
    try:
        from redis_manager import RedisManager
        
        redis_url = os.environ.get('REDIS_URL')
        if not redis_url:
            print("⚠️  REDIS_URL not set, using localhost")
        
        rm = RedisManager()
        print("✓ Redis connection established")
        
        # Test queue operations
        rm.clear_queue()
        print("✓ Queue cleared")
        
        test_url = 'https://test.example.com'
        rm.enqueue_url(test_url)
        print(f"✓ Enqueued test URL: {test_url}")
        
        size = rm.queue_size()
        print(f"✓ Queue size: {size}")
        
        dequeued = rm.dequeue_url()
        print(f"✓ Dequeued: {dequeued}")
        
        if dequeued == test_url:
            print("✅ Redis tests passed!\n")
            return True
        else:
            print("❌ Dequeued URL doesn't match!\n")
            return False
            
    except ImportError:
        print("❌ Could not import RedisManager")
        return False
    except Exception as e:
        print(f"❌ Redis test failed: {e}\n")
        return False


def test_database():
    """Test PostgreSQL connection and tables"""
    print("="*60)
    print("TESTING DATABASE")
    print("="*60)
    
    try:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not set")
            return False
        
        conn = psycopg2.connect(db_url)
        print("✓ Database connection established")
        
        cursor = conn.cursor()
        
        # Check for required tables
        tables = ['pages', 'links']
        for table in tables:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table,))
            
            exists = cursor.fetchone()[0]
            if exists:
                print(f"✓ Table '{table}' exists")
            else:
                print(f"⚠️  Table '{table}' does not exist (run SQL schema)")
        
        # Check page count
        try:
            cursor.execute("SELECT COUNT(*) FROM pages")
            count = cursor.fetchone()[0]
            print(f"✓ Current pages in database: {count}")
        except:
            print("⚠️  Could not count pages (table may not exist)")
        
        cursor.close()
        conn.close()
        
        print("✅ Database tests passed!\n")
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}\n")
        return False


def test_seed_file():
    """Test seed URLs file"""
    print("="*60)
    print("TESTING SEED FILE")
    print("="*60)
    
    try:
        with open('seed_urls.txt', 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        print(f"✓ Found {len(urls)} seed URLs")
        
        if len(urls) >= 20:
            print(f"✓ Sufficient seed URLs ({len(urls)} >= 20)")
        else:
            print(f"⚠️  Few seed URLs ({len(urls)} < 20)")
        
        # Test a few URLs against blocklist
        blocklist = get_blocklist()
        blocked_count = 0
        
        for url in urls[:5]:  # Test first 5
            is_blocked, _ = blocklist.is_blocked(url)
            if is_blocked:
                blocked_count += 1
                print(f"⚠️  Seed URL is blocked: {url}")
        
        if blocked_count == 0:
            print("✓ Sample seed URLs are not blocked")
        else:
            print(f"⚠️  {blocked_count} seed URLs are blocked!")
        
        print("✅ Seed file tests passed!\n")
        return True
        
    except FileNotFoundError:
        print("❌ seed_urls.txt not found\n")
        return False
    except Exception as e:
        print(f"❌ Seed file test failed: {e}\n")
        return False


def main():
    """Run all tests"""
    print("\n" + "🔍 NotHere.one Crawler Component Tests" + "\n")
    
    results = []
    
    # Run tests
    results.append(("Blocklist", test_blocklist()))
    results.append(("Redis", test_redis()))
    results.append(("Database", test_database()))
    results.append(("Seed File", test_seed_file()))
    
    # Summary
    print("="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {name}")
    
    all_passed = all(result[1] for result in results)
    
    print("="*60)
    
    if all_passed:
        print("\n🎉 All tests passed! Ready to crawl.\n")
        print("To start crawling, run:")
        print("  python crawler.py --seed seed_urls.txt --max-pages 10\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please fix issues before crawling.\n")
        return 1


if __name__ == '__main__':
    sys.exit(main())
