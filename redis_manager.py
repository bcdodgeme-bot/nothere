"""
RedisManager for NotHere.one crawler
Handles URL queue and caching operations
"""

import os
import redis
import logging

logger = logging.getLogger(__name__)


class RedisManager:
    """
    Manages Redis operations for the crawler
    """
    
    def __init__(self, redis_url=None):
        """Initialize Redis connection"""
        if redis_url is None:
            redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
        
        # Parse Redis URL and connect
        self.client = redis.from_url(redis_url, decode_responses=True)
        
        # Queue key
        self.queue_key = 'crawler:queue'
        
        # Test connection
        try:
            self.client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def enqueue_url(self, url):
        """
        Add URL to the crawl queue
        Uses LPUSH for FIFO queue behavior
        """
        try:
            # Use SADD to prevent duplicates in queue
            added = self.client.sadd(f'{self.queue_key}:set', url)
            
            if added:
                # Add to list for processing
                self.client.lpush(self.queue_key, url)
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error enqueueing URL {url}: {e}")
            return False
    
    def dequeue_url(self):
        """
        Get next URL from the crawl queue
        Uses RPOP for FIFO behavior
        Returns None if queue is empty
        """
        try:
            url = self.client.rpop(self.queue_key)
            return url
        except Exception as e:
            logger.error(f"Error dequeuing URL: {e}")
            return None
    
    def queue_size(self):
        """Get current queue size"""
        try:
            return self.client.llen(self.queue_key)
        except Exception as e:
            logger.error(f"Error getting queue size: {e}")
            return 0
    
    def clear_queue(self):
        """Clear the entire queue"""
        try:
            self.client.delete(self.queue_key)
            self.client.delete(f'{self.queue_key}:set')
            logger.info("Queue cleared")
        except Exception as e:
            logger.error(f"Error clearing queue: {e}")
    
    def is_url_queued(self, url):
        """Check if URL is already in queue"""
        try:
            return self.client.sismember(f'{self.queue_key}:set', url)
        except Exception as e:
            logger.error(f"Error checking if URL queued: {e}")
            return False
    
    def cache_set(self, key, value, ttl=3600):
        """Set a cached value with TTL (default 1 hour)"""
        try:
            self.client.setex(key, ttl, value)
        except Exception as e:
            logger.error(f"Error setting cache: {e}")
    
    def cache_get(self, key):
        """Get a cached value"""
        try:
            return self.client.get(key)
        except Exception as e:
            logger.error(f"Error getting cache: {e}")
            return None
    
    def get_stats(self):
        """Get Redis stats"""
        return {
            'queue_size': self.queue_size(),
            'queue_set_size': self.client.scard(f'{self.queue_key}:set')
        }


if __name__ == '__main__':
    # Test the RedisManager
    logging.basicConfig(level=logging.INFO)
    
    print("Testing RedisManager...")
    
    try:
        rm = RedisManager()
        
        # Clear queue first
        rm.clear_queue()
        print(f"Initial queue size: {rm.queue_size()}")
        
        # Test enqueue
        test_urls = [
            'https://example.com',
            'https://example.org',
            'https://example.net'
        ]
        
        for url in test_urls:
            rm.enqueue_url(url)
            print(f"Enqueued: {url}")
        
        print(f"Queue size after enqueuing: {rm.queue_size()}")
        
        # Test dequeue
        while True:
            url = rm.dequeue_url()
            if url is None:
                break
            print(f"Dequeued: {url}")
        
        print(f"Final queue size: {rm.queue_size()}")
        
        # Test stats
        print(f"Stats: {rm.get_stats()}")
        
        print("\n✅ RedisManager test completed successfully")
        
    except Exception as e:
        print(f"\n❌ RedisManager test failed: {e}")