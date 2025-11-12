"""
Tier 1 Hard Blocklist for NotHere.one
Filters harmful content before crawling
"""

from urllib.parse import urlparse
import re


class Tier1Blocklist:
    """
    Tier 1 filtering: Hard blocks for clearly harmful content
    Checks before fetching to avoid wasting resources
    """
    
    def __init__(self):
        # Domain-level blocks (exact matches)
        self.blocked_domains = {
            # Adult content
            'pornhub.com', 'xvideos.com', 'xnxx.com', 'redtube.com', 'youporn.com',
            'tube8.com', 'spankbang.com', 'xhamster.com', 'eporner.com', 'motherless.com',
            'livejasmin.com', 'chaturbate.com', 'cam4.com', 'stripchat.com', 'camsoda.com',
            
            # Gambling
            'bet365.com', 'pokerstars.com', 'bwin.com', 'draftkings.com', 'fanduel.com',
            'betfair.com', 'casino.com', 'bovada.lv', '888casino.com', 'williamhill.com',
            'paddypower.com', 'betway.com', 'unibet.com', 'betonline.ag',
            
            # Alcohol/drug promotion
            'totalwine.com', 'wine.com', 'drizly.com', 'reservebar.com',
            'leafly.com', 'weedmaps.com', 'eaze.com',
            
            # Payday loans & predatory lending
            'cashnetusa.com', 'checkintocash.com', 'cashadvance.com', 'speedycash.com',
            'moneylion.com', 'advanceamerica.net', 'titlemax.com', 'checkngo.com',
            
            # Known MLM/pyramid schemes
            'amway.com', 'herbalife.com', 'monat.com', 'lularoe.com', 'itworks.com',
            'younique.com', 'avon.com', 'marykay.com', 'arbonne.com', 'beachbody.com',
            'rodan-fields.com', 'pampered-chef.com', 'usana.com', 'isagenix.com',
            
            # Hate groups (SPLC-flagged examples)
            'stormfront.org', 'dailystormer.name', 'vdare.com', 'americanrenaissance.com',
            'counter-currents.com', 'theoccidentalobserver.net', 'unz.com',
            
            # Misinformation & conspiracy
            'infowars.com', 'prisonplanet.com', 'naturalnews.com', 'beforeitsnews.com',
            'veteranstoday.com', 'rense.com', 'davidicke.com', 'thetruthseeker.co.uk',
            'conspiracyplanet.com', 'rumormillnews.com', 'qmap.pub', 'qanon.pub',
            'thegatewaypundit.com', 'tfrlive.com', 'theepochtimes.com',
            
            # Antivax & dangerous pseudoscience
            'naturalnews.com', 'mercola.com', 'greenmedinfo.com', 'tenpenny.com',
            'learntherisk.org', 'childrenshealthdefense.org', 'nvic.org',
            
            # Content farms & spam
            'ehow.com', 'answers.com', 'ask.com', 'answerbag.com', 'chacha.com',
            'mahalo.com', 'wikihow.com', 'buzzle.com', 'listverse.com',
        }
        
        # Blocked TLDs
        self.blocked_tlds = {
            '.xxx', '.adult', '.porn', '.sex', '.sexy', '.casino', '.bet',
            '.poker', '.loan', '.loans', '.date', '.download', '.click'
        }
        
        # URL path/keyword patterns (regex)
        self.blocked_patterns = [
            # Adult content indicators
            r'/porn/',
            r'/xxx/',
            r'/sex/',
            r'/adult/',
            r'/nude/',
            r'/escort/',
            r'/hookup/',
            r'/camgirl/',
            r'/onlyfans/',
            
            # Gambling
            r'/casino/',
            r'/poker/',
            r'/betting/',
            r'/slots/',
            r'/blackjack/',
            r'/roulette/',
            
            # Drugs/alcohol
            r'/buy-weed/',
            r'/marijuana-delivery/',
            r'/liquor-store/',
            r'/order-alcohol/',
            
            # Payday loans
            r'/payday-loan/',
            r'/cash-advance/',
            r'/title-loan/',
            r'/quick-cash/',
            
            # MLM indicators
            r'/join-my-team/',
            r'/be-your-own-boss/',
            r'/work-from-home-opportunity/',
            
            # Conspiracy/misinformation keywords
            r'/flat-earth/',
            r'/holocaust-hoax/',
            r'/crisis-actors/',
            r'/false-flag/',
            r'/qanon/',
            r'/moon-landing-fake/',
            r'/chemtrails/',
            r'/5g-conspiracy/',
            r'/covid-hoax/',
            r'/plandemic/',
            r'/vaccine-injury/',
            r'/anti-vax/',
        ]
        
        # Compile regex patterns for efficiency
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.blocked_patterns]
        
    def is_blocked(self, url):
        """
        Check if URL should be blocked
        Returns: (is_blocked: bool, reason: str)
        """
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc
            
            # Remove www. prefix for checking
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Check exact domain match
            if domain in self.blocked_domains:
                return True, f"Blocked domain: {domain}"
            
            # Check if it's a subdomain of a blocked domain
            for blocked in self.blocked_domains:
                if domain.endswith('.' + blocked) or domain == blocked:
                    return True, f"Blocked domain: {blocked}"
            
            # Check TLD
            for tld in self.blocked_tlds:
                if domain.endswith(tld):
                    return True, f"Blocked TLD: {tld}"
            
            # Check URL path patterns
            full_url = url.lower()
            for pattern in self.compiled_patterns:
                if pattern.search(full_url):
                    return True, f"Blocked pattern: {pattern.pattern}"
            
            return False, None
            
        except Exception as e:
            # If we can't parse the URL, block it to be safe
            return True, f"Invalid URL format: {str(e)}"
    
    def add_domain(self, domain):
        """Add a domain to the blocklist"""
        domain = domain.lower().strip()
        if domain.startswith('www.'):
            domain = domain[4:]
        self.blocked_domains.add(domain)
    
    def add_pattern(self, pattern):
        """Add a URL pattern to the blocklist"""
        compiled = re.compile(pattern, re.IGNORECASE)
        self.compiled_patterns.append(compiled)
        self.blocked_patterns.append(pattern)
    
    def remove_domain(self, domain):
        """Remove a domain from the blocklist"""
        domain = domain.lower().strip()
        if domain.startswith('www.'):
            domain = domain[4:]
        self.blocked_domains.discard(domain)
    
    def get_stats(self):
        """Get blocklist statistics"""
        return {
            'blocked_domains': len(self.blocked_domains),
            'blocked_tlds': len(self.blocked_tlds),
            'blocked_patterns': len(self.blocked_patterns)
        }


# Singleton instance
_blocklist_instance = None

def get_blocklist():
    """Get or create the blocklist singleton"""
    global _blocklist_instance
    if _blocklist_instance is None:
        _blocklist_instance = Tier1Blocklist()
    return _blocklist_instance


if __name__ == '__main__':
    # Test the blocklist
    blocklist = get_blocklist()
    
    test_urls = [
        'https://wikipedia.org',
        'https://pornhub.com',
        'https://example.com/casino/games',
        'https://bbc.com',
        'https://example.xxx',
        'https://qanon.pub',
        'https://nytimes.com',
        'https://example.com/flat-earth/proof',
    ]
    
    print("Tier 1 Blocklist Test\n" + "="*50)
    print(f"Stats: {blocklist.get_stats()}\n")
    
    for url in test_urls:
        is_blocked, reason = blocklist.is_blocked(url)
        status = "❌ BLOCKED" if is_blocked else "✅ ALLOWED"
        print(f"{status}: {url}")
        if reason:
            print(f"  Reason: {reason}")
        print()