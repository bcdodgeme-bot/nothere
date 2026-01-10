"""
Example: Customizing the Tier 1 Blocklist
Shows how to add/remove domains and patterns dynamically
"""

from blocklist import get_blocklist


def example_add_domains():
    """Add custom domains to the blocklist"""
    blocklist = get_blocklist()
    
    # Add individual domains
    custom_domains = [
        'spamsite.com',
        'scamwebsite.net',
        'badnews.io'
    ]
    
    print("Adding custom domains...")
    for domain in custom_domains:
        blocklist.add_domain(domain)
        print(f"  ✓ Added: {domain}")
    
    print(f"\nTotal blocked domains: {len(blocklist.blocked_domains)}")


def example_add_patterns():
    """Add custom URL patterns to the blocklist"""
    blocklist = get_blocklist()
    
    # Add custom patterns
    custom_patterns = [
        r'/spam/',
        r'/phishing/',
        r'/get-rich-quick/'
    ]
    
    print("\nAdding custom patterns...")
    for pattern in custom_patterns:
        blocklist.add_pattern(pattern)
        print(f"  ✓ Added pattern: {pattern}")
    
    print(f"\nTotal blocked patterns: {len(blocklist.blocked_patterns)}")


def example_test_custom_blocks():
    """Test URLs against customized blocklist"""
    blocklist = get_blocklist()
    
    test_urls = [
        'https://spamsite.com',
        'https://example.com/spam/page',
        'https://legitimate.com',
        'https://example.com/get-rich-quick/scheme'
    ]
    
    print("\n" + "="*60)
    print("Testing custom blocklist")
    print("="*60)
    
    for url in test_urls:
        is_blocked, reason = blocklist.is_blocked(url)
        
        if is_blocked:
            print(f"\n❌ BLOCKED: {url}")
            print(f"   Reason: {reason}")
        else:
            print(f"\n✅ ALLOWED: {url}")


def example_remove_domain():
    """Example of removing a domain from blocklist"""
    blocklist = get_blocklist()
    
    # Maybe you want to temporarily allow a blocked domain
    domain_to_allow = 'example.com'
    
    print(f"\nRemoving {domain_to_allow} from blocklist...")
    blocklist.remove_domain(domain_to_allow)
    print(f"✓ Removed")


def example_batch_add_from_file():
    """Add domains from a custom blocklist file"""
    blocklist = get_blocklist()
    
    # Example: Load from a custom file
    custom_file = 'custom_blocklist.txt'
    
    try:
        with open(custom_file, 'r') as f:
            for line in f:
                domain = line.strip()
                if domain and not domain.startswith('#'):
                    blocklist.add_domain(domain)
                    print(f"  ✓ Added: {domain}")
        
        print(f"\n✓ Loaded domains from {custom_file}")
    
    except FileNotFoundError:
        print(f"\nFile not found: {custom_file}")
        print("Create it with domains, one per line:")
        print("  badsite1.com")
        print("  badsite2.net")
        print("  # Comments start with #")


def example_get_stats():
    """Show current blocklist statistics"""
    blocklist = get_blocklist()
    stats = blocklist.get_stats()
    
    print("\n" + "="*60)
    print("BLOCKLIST STATISTICS")
    print("="*60)
    print(f"Blocked domains:  {stats['blocked_domains']}")
    print(f"Blocked TLDs:     {stats['blocked_tlds']}")
    print(f"Blocked patterns: {stats['blocked_patterns']}")
    print("="*60)


if __name__ == '__main__':
    print("Tier 1 Blocklist Customization Examples")
    print("="*60)
    
    # Show initial stats
    example_get_stats()
    
    # Add custom domains
    example_add_domains()
    
    # Add custom patterns
    example_add_patterns()
    
    # Test the customized blocklist
    example_test_custom_blocks()
    
    # Show how to batch load
    print("\n" + "="*60)
    print("Batch loading example:")
    print("="*60)
    example_batch_add_from_file()
    
    # Final stats
    example_get_stats()
    
    print("\n✅ Examples complete!")
    print("\nNOTE: Changes made here are only in memory.")
    print("To persist changes, modify blocklist.py directly.")