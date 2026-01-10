"""
NotHere.one Composite Scoring System
=====================================
Evaluates crawled pages across 5 dimensions:
1. Islamic Alignment (-100 to +100, normalized to 0-100) - 30% weight
2. Quality Score (0-100) - 25% weight
3. Authority Score (0-100) - 20% weight
4. Media Literacy Score (0-100, AI-powered) - 15% weight
5. Equity Boost (0-30 bonus points) - 10% weight

Final Score: 0-100 (determines indexing and ranking)
- Score < 25: Do not index
- Score 25-40: Index, low rank
- Score 40-50: Index, medium rank
- Score 50+: Index, high rank

Override: SPLC/ACLU/CAIR flagged = instant 0 (never index)
"""

import re
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
from collections import defaultdict
import json

# Optional dependencies (graceful degradation)
try:
    import textstat
    HAS_TEXTSTAT = True
except ImportError:
    HAS_TEXTSTAT = False
    logging.warning("textstat not available, using simplified readability scoring")

logger = logging.getLogger(__name__)


class CompositeScorer:
    """
    Unified scoring system for NotHere.one
    Evaluates pages across all dimensions and outputs 0-100 score
    """
    
    def __init__(self, db_conn):
        """
        Initialize scorer with database connection
        
        Args:
            db_conn: psycopg2 database connection
        """
        self.db_conn = db_conn
        
        # Caches for performance
        self._keyword_cache = None
        self._domain_authority_cache = {}
        self._equity_cache = {}
        self._blocklist_cache = {}
        
        # Category weights for Islamic alignment
        self.category_weights = {
            'haram_prohibited': -10,    # Strong negative
            'halal_encouraged': +5,      # Positive
            'core_values': +3,           # Positive
            'social_ethics': +3          # Positive
        }
        
        # Educational/research domains (context-aware scoring)
        self.educational_tlds = {'.edu', '.gov', '.ac.uk', '.ac.in', '.edu.au'}
        self.news_domains = {
            'bbc.com', 'bbc.co.uk', 'reuters.com', 'apnews.com',
            'ap.org', 'aljazeera.com', 'npr.org', 'pbs.org'
        }
        
        # Legitimate publications that might trigger false positives
        self.false_positive_patterns = [
            r'\bbitch\s+magazine\b',  # Feminist publication, not profanity
            r'\bthe\s+intercept\b',   # News outlet
        ]
        
        logger.info("CompositeScorer initialized")
    
    # ========================================================================
    # ISLAMIC ALIGNMENT SCORING
    # ========================================================================
    
    def _load_keywords_from_db(self):
        """
        Load all keywords and their themes from database
        Returns dict: {keyword: [(theme_id, principle, category), ...]}
        """
        if self._keyword_cache is not None:
            return self._keyword_cache
        
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                SELECT tk.keyword, tk.theme_id, it.principle, it.category
                FROM theme_keywords tk
                JOIN islamic_themes it ON tk.theme_id = it.id
                ORDER BY tk.keyword
            """)
            
            # Build keyword -> themes mapping
            keyword_map = defaultdict(list)
            for keyword, theme_id, principle, category in cursor.fetchall():
                keyword_map[keyword.lower()].append({
                    'theme_id': theme_id,
                    'principle': principle,
                    'category': category,
                    'weight': self.category_weights.get(category, 0)
                })
            
            self._keyword_cache = dict(keyword_map)
            logger.info(f"Loaded {len(self._keyword_cache)} unique keywords from database")
            return self._keyword_cache
            
        finally:
            cursor.close()
    
    def _detect_context_signals(self, content, domain):
        """
        Detect if content is educational/research context
        Returns context flags dict
        """
        content_lower = content.lower()
        
        context = {
            'is_educational': False,
            'is_news': False,
            'is_research': False,
            'is_false_positive': False
        }
        
        # Check domain
        parsed = urlparse(domain if '://' in domain else f'https://{domain}')
        domain_clean = parsed.netloc.lower().replace('www.', '')
        
        # Educational domains
        for tld in self.educational_tlds:
            if domain_clean.endswith(tld):
                context['is_educational'] = True
                break
        
        # News domains
        if any(news in domain_clean for news in self.news_domains):
            context['is_news'] = True
        
        # Research indicators
        research_terms = ['research', 'study', 'paper', 'journal', 'academic',
                         'university', 'scholar', 'peer-reviewed', 'abstract']
        if any(term in content_lower for term in research_terms):
            context['is_research'] = True
        
        # False positive patterns
        for pattern in self.false_positive_patterns:
            if re.search(pattern, content_lower):
                context['is_false_positive'] = True
                break
        
        return context
    
    def _match_keywords_in_content(self, content, keyword_map):
        """
        Match keywords in content with whole-word matching
        Returns list of matches: [(keyword, theme_info), ...]
        """
        content_lower = content.lower()
        matches = []
        
        for keyword, theme_list in keyword_map.items():
            # Whole-word regex pattern
            pattern = r'\b' + re.escape(keyword) + r'\b'
            
            if re.search(pattern, content_lower):
                for theme_info in theme_list:
                    matches.append((keyword, theme_info))
        
        return matches
    
    def calculate_islamic_alignment(self, content, domain):
        """
        Calculate Islamic alignment score
        
        Returns:
            tuple: (score: int, matched_themes: dict)
            - score: -100 to +100 (normalized to 0-100 later)
            - matched_themes: details for transparency
        """
        if not content or len(content.strip()) < 50:
            return 0, {'reason': 'content_too_short'}
        
        # Load keywords
        keyword_map = self._load_keywords_from_db()
        
        # Detect context
        context = self._detect_context_signals(content, domain)
        
        # Match keywords
        matches = self._match_keywords_in_content(content, keyword_map)
        
        if not matches:
            return 0, {'reason': 'no_keywords_matched'}
        
        # Calculate score by category
        category_scores = defaultdict(lambda: {'count': 0, 'weight': 0, 'total': 0})
        matched_themes_detail = []
        
        for keyword, theme_info in matches:
            category = theme_info['category']
            weight = theme_info['weight']
            
            # Apply context-aware weight reduction for negative categories
            if weight < 0:
                if context['is_educational'] or context['is_research']:
                    weight = weight * 0.3  # 70% reduction for academic content
                elif context['is_news']:
                    weight = weight * 0.5  # 50% reduction for news reporting
                
                if context['is_false_positive']:
                    weight = 0  # Neutralize false positives
            
            category_scores[category]['count'] += 1
            category_scores[category]['weight'] = theme_info['weight']
            category_scores[category]['total'] += weight
            
            matched_themes_detail.append({
                'keyword': keyword,
                'theme': theme_info['principle'],
                'category': category,
                'weight': weight
            })
        
        # Sum total score
        raw_score = sum(cat['total'] for cat in category_scores.values())
        
        # Normalize to -100 to +100 range
        # Assume max ~50 matches * max weight (10) = 500 theoretical max
        # Clamp to -100 to +100
        normalized_score = max(-100, min(100, raw_score))
        
        matched_themes = {
            'raw_score': raw_score,
            'normalized_score': normalized_score,
            'matches_count': len(matches),
            'categories': dict(category_scores),
            'context': context,
            'top_matches': matched_themes_detail[:20]  # Keep top 20 for logging
        }
        
        logger.debug(f"Islamic alignment: {normalized_score} (raw: {raw_score}, matches: {len(matches)})")
        
        return normalized_score, matched_themes
    
    # ========================================================================
    # QUALITY SCORING
    # ========================================================================
    
    def _calculate_readability(self, content):
        """Calculate readability score (0-15 points)"""
        if HAS_TEXTSTAT:
            try:
                # Flesch Reading Ease: 0-100 (higher = easier)
                ease = textstat.flesch_reading_ease(content)
                
                # Convert to points
                if ease >= 60:
                    return 15  # Easy to read
                elif ease >= 30:
                    return 10  # Moderate
                else:
                    return 5   # Difficult
            except:
                pass
        
        # Fallback: Simple approximation
        words = content.split()
        sentences = content.count('.') + content.count('!') + content.count('?')
        
        if sentences == 0:
            return 5
        
        avg_words_per_sentence = len(words) / sentences
        
        if avg_words_per_sentence <= 15:
            return 15  # Short sentences, easier
        elif avg_words_per_sentence <= 25:
            return 10  # Medium
        else:
            return 5   # Long sentences, harder
    
    def _calculate_content_length_score(self, content):
        """Calculate content length score (0-10 points)"""
        word_count = len(content.split())
        
        if word_count < 100:
            return 0  # Thin content
        elif word_count < 500:
            return 5
        elif word_count <= 2000:
            return 10  # Ideal range
        else:
            return 8   # Very long, might be spam
    
    def _calculate_structural_quality(self, content):
        """Calculate structural quality (0-15 points)"""
        score = 0
        
        # Check for headings (h1, h2 tags would be stripped, so we approximate)
        # Look for patterns like lines in ALL CAPS or with special formatting
        lines = content.split('\n')
        has_structure = any(len(line) < 100 and line.isupper() for line in lines)
        
        if has_structure:
            score += 5
        
        # Check for lists or structured content
        if content.count('\n') > 5:  # Multiple paragraphs
            score += 5
        
        # Not a content farm pattern (avoid excessive repetition)
        words = content.lower().split()
        if len(words) > 50:
            unique_ratio = len(set(words)) / len(words)
            if unique_ratio > 0.4:  # At least 40% unique words
                score += 5
        
        return score
    
    def calculate_quality_score(self, content, domain, title):
        """
        Calculate quality score (0-100)
        
        Components:
        - Readability: 0-15 points
        - Content length: 0-10 points
        - Structural quality: 0-15 points
        - Technical quality: 0-30 points (SSL + domain features)
        - Freshness: 0-15 points (from crawl date)
        - Grammar/uniqueness: 0-15 points
        
        Returns:
            tuple: (score: int, details: dict)
        """
        if not content or len(content.strip()) < 50:
            return 0, {'reason': 'content_too_short'}
        
        details = {}
        total_score = 0
        
        # A. Content Quality (0-40 points)
        readability = self._calculate_readability(content)
        details['readability'] = readability
        total_score += readability
        
        length_score = self._calculate_content_length_score(content)
        details['content_length'] = length_score
        total_score += length_score
        
        structural = self._calculate_structural_quality(content)
        details['structural_quality'] = structural
        total_score += structural
        
        # Grammar/uniqueness (basic check)
        words = content.split()
        if len(words) > 50:
            unique_ratio = len(set(words)) / len(words)
            grammar_score = min(15, int(unique_ratio * 20))
        else:
            grammar_score = 5
        details['grammar_uniqueness'] = grammar_score
        total_score += grammar_score
        
        # B. Technical Quality (0-30 points)
        technical_score = 0
        
        # SSL check (HTTPS)
        if 'https://' in domain.lower():
            technical_score += 10
            details['has_ssl'] = True
        else:
            details['has_ssl'] = False
        
        # Domain age (use first crawl date from DB)
        domain_age_score = self._get_domain_age_score(domain)
        technical_score += domain_age_score
        details['domain_age_score'] = domain_age_score
        
        # Mobile optimization (assume yes for now, would need actual check)
        technical_score += 5
        details['mobile_optimized'] = True
        
        total_score += technical_score
        
        # C. Freshness (0-15 points) - calculated from crawled_at in main function
        # Will be added by caller
        
        # Total out of 100
        details['total'] = min(100, total_score)
        
        logger.debug(f"Quality score: {details['total']} (readability={readability}, length={length_score})")
        
        return min(100, total_score), details
    
    def _get_domain_age_score(self, domain):
        """
        Get domain age score from first crawl date (0-15 points)
        """
        cursor = self.db_conn.cursor()
        try:
            parsed = urlparse(domain if '://' in domain else f'https://{domain}')
            domain_clean = parsed.netloc
            
            cursor.execute("""
                SELECT MIN(crawled_at) as first_seen
                FROM pages
                WHERE domain = %s
            """, (domain_clean,))
            
            result = cursor.fetchone()
            if not result or not result[0]:
                return 5  # Default for new domains
            
            first_seen = result[0]
            age = datetime.now() - first_seen
            
            # Score based on age in our database
            if age.days < 7:
                return 0   # Very new, suspicious
            elif age.days < 30:
                return 5
            elif age.days < 90:
                return 10
            else:
                return 15  # Established
            
        except Exception as e:
            logger.error(f"Error getting domain age: {e}")
            return 5
        finally:
            cursor.close()
    
    # ========================================================================
    # AUTHORITY SCORING
    # ========================================================================
    
    def _get_tld_score(self, domain):
        """Calculate TLD prestige score (0-50 points)"""
        parsed = urlparse(domain if '://' in domain else f'https://{domain}')
        domain_clean = parsed.netloc.lower()
        
        if domain_clean.endswith('.gov'):
            return 50
        elif domain_clean.endswith('.edu'):
            return 45
        elif domain_clean.endswith(('.ac.uk', '.ac.in', '.edu.au')):
            return 45
        elif domain_clean.endswith('.org'):
            return 30
        elif domain_clean.endswith(('.com', '.net')):
            return 20
        else:
            return 10
    
    def _get_backlink_score(self, url):
        """Calculate backlink score (0-30 points)"""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                SELECT COUNT(DISTINCT source_page_id) as backlink_count
                FROM links
                WHERE target_url = %s
            """, (url,))
            
            result = cursor.fetchone()
            backlink_count = result[0] if result else 0
            
            # Score based on backlink count
            if backlink_count == 0:
                return 0
            elif backlink_count <= 5:
                return 10
            elif backlink_count <= 20:
                return 20
            else:
                return 30
            
        except Exception as e:
            logger.error(f"Error counting backlinks: {e}")
            return 0
        finally:
            cursor.close()
    
    def _get_external_authority_score(self, url, domain):
        """Calculate external authority signals (0-20 points)"""
        cursor = self.db_conn.cursor()
        try:
            score = 0
            
            # Check if referenced by .edu domains
            cursor.execute("""
                SELECT COUNT(DISTINCT p.domain) as edu_refs
                FROM links l
                JOIN pages p ON l.source_page_id = p.id
                WHERE l.target_url = %s
                    AND p.domain LIKE '%.edu'
            """, (url,))
            
            edu_refs = cursor.fetchone()[0]
            if edu_refs > 0:
                score += 10
            
            # Check if referenced by .gov domains
            cursor.execute("""
                SELECT COUNT(DISTINCT p.domain) as gov_refs
                FROM links l
                JOIN pages p ON l.source_page_id = p.id
                WHERE l.target_url = %s
                    AND p.domain LIKE '%.gov'
            """, (url,))
            
            gov_refs = cursor.fetchone()[0]
            if gov_refs > 0:
                score += 10
            
            return min(20, score)
            
        except Exception as e:
            logger.error(f"Error calculating external authority: {e}")
            return 0
        finally:
            cursor.close()
    
    def calculate_authority_score(self, url, domain):
        """
        Calculate authority score (0-100)
        
        Components:
        - TLD prestige: 0-50 points
        - Backlinks: 0-30 points
        - External authority: 0-20 points
        
        Returns:
            tuple: (score: int, details: dict)
        """
        # Check cache
        cache_key = domain
        if cache_key in self._domain_authority_cache:
            cached = self._domain_authority_cache[cache_key]
            # Cache for 7 days
            if (datetime.now() - cached['timestamp']).days < 7:
                return cached['score'], cached['details']
        
        details = {}
        
        # TLD score
        tld_score = self._get_tld_score(domain)
        details['tld_score'] = tld_score
        
        # Backlink score
        backlink_score = self._get_backlink_score(url)
        details['backlink_score'] = backlink_score
        
        # External authority
        external_score = self._get_external_authority_score(url, domain)
        details['external_authority_score'] = external_score
        
        total = tld_score + backlink_score + external_score
        details['total'] = total
        
        # Cache result
        self._domain_authority_cache[cache_key] = {
            'score': total,
            'details': details,
            'timestamp': datetime.now()
        }
        
        logger.debug(f"Authority score: {total} (TLD={tld_score}, backlinks={backlink_score})")
        
        return total, details
    
    # ========================================================================
    # EQUITY BOOST
    # ========================================================================
    
    def calculate_equity_boost(self, domain):
        """
        Calculate equity boost (0-30 bonus points)
        
        Checks equity_domains table for certifications:
        - minority_owned: +15
        - women_owned: +15
        - veteran_owned: +15
        - b_corp: +10
        - lgbtq_owned: +15
        - disability_owned: +15
        (Boosts can stack, max 30)
        
        Returns:
            tuple: (boost: int, details: dict)
        """
        parsed = urlparse(domain if '://' in domain else f'https://{domain}')
        domain_clean = parsed.netloc.lower().replace('www.', '')
        
        # Check cache
        if domain_clean in self._equity_cache:
            return self._equity_cache[domain_clean]
        
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                SELECT minority_owned, women_owned, veteran_owned, 
                       b_corp, lgbtq_owned, disability_owned
                FROM equity_domains
                WHERE domain = %s
            """, (domain_clean,))
            
            result = cursor.fetchone()
            if not result:
                self._equity_cache[domain_clean] = (0, {'reason': 'not_in_equity_list'})
                return 0, {'reason': 'not_in_equity_list'}
            
            minority, women, veteran, bcorp, lgbtq, disability = result
            
            boost = 0
            details = {}
            
            if minority:
                boost += 15
                details['minority_owned'] = True
            if women:
                boost += 15
                details['women_owned'] = True
            if veteran:
                boost += 15
                details['veteran_owned'] = True
            if bcorp:
                boost += 10
                details['b_corp'] = True
            if lgbtq:
                boost += 15
                details['lgbtq_owned'] = True
            if disability:
                boost += 15
                details['disability_owned'] = True
            
            # Cap at 30
            boost = min(30, boost)
            details['total_boost'] = boost
            
            result = (boost, details)
            self._equity_cache[domain_clean] = result
            
            if boost > 0:
                logger.info(f"Equity boost: +{boost} points for {domain_clean}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking equity domains: {e}")
            return 0, {'error': str(e)}
        finally:
            cursor.close()
    
    # ========================================================================
    # ORGANIZATIONAL BLOCKLIST
    # ========================================================================
    
    def check_org_blocklist(self, domain):
        """
        Check if domain is flagged by civil rights orgs
        
        Returns:
            tuple: (is_blocked: bool, reason: str)
        """
        parsed = urlparse(domain if '://' in domain else f'https://{domain}')
        domain_clean = parsed.netloc.lower().replace('www.', '')
        
        # Check cache
        if domain_clean in self._blocklist_cache:
            return self._blocklist_cache[domain_clean]
        
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                SELECT splc_flagged, aclu_flagged, cair_flagged, 
                       adl_flagged, other_org_flagged, reason
                FROM org_blocklist
                WHERE domain = %s
            """, (domain_clean,))
            
            result = cursor.fetchone()
            if not result:
                self._blocklist_cache[domain_clean] = (False, None)
                return False, None
            
            splc, aclu, cair, adl, other, reason = result
            
            if any([splc, aclu, cair, adl, other]):
                flags = []
                if splc: flags.append('SPLC')
                if aclu: flags.append('ACLU')
                if cair: flags.append('CAIR')
                if adl: flags.append('ADL')
                if other: flags.append('Other')
                
                block_reason = f"Flagged by: {', '.join(flags)}"
                if reason:
                    block_reason += f" - {reason}"
                
                result = (True, block_reason)
                self._blocklist_cache[domain_clean] = result
                
                logger.warning(f"Domain blocked: {domain_clean} - {block_reason}")
                return result
            
            self._blocklist_cache[domain_clean] = (False, None)
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking org blocklist: {e}")
            return False, None
        finally:
            cursor.close()
    
    # ========================================================================
    # MEDIA LITERACY (STUB)
    # ========================================================================
    
    def calculate_media_literacy_score(self, content, domain, title=None):
        """
        Media literacy score using AI-powered pattern detection
        
        Detects:
        - Scientific consensus mismatch
        - Extraordinary claims without evidence
        - Statistical manipulation
        - Source-expertise mismatch
        - Conflict of interest
        - Historical revisionism
        - Predatory economic patterns
        
        Cost-optimized: Only analyzes content with red flag keywords
        
        Returns:
            tuple: (score: int, details: dict)
        """
        try:
            from media_literacy_scorer import calculate_media_literacy_score
            
            # Pass all parameters including title
            score, details = calculate_media_literacy_score(
                content=content,
                domain=domain,
                title=title,
                db_conn=self.db_conn
            )
            
            return score, details
            
        except ImportError:
            logger.warning("media_literacy_scorer not found, returning neutral score")
            return 50, {'status': 'stub', 'note': 'Scorer module not available'}
        except Exception as e:
            logger.error(f"Media literacy scoring failed: {e}")
            # Graceful degradation - return neutral score
            return 50, {'status': 'error', 'error': str(e)}
    
    # ========================================================================
    # COMPOSITE CALCULATION
    # ========================================================================
    
    def calculate_composite_score(self, page_id, url, title, content, domain, crawled_at):
        """
        Calculate final composite score
        
        Weights:
        - Islamic alignment: 30%
        - Quality: 25%
        - Authority: 20%
        - Media literacy: 15%
        - Equity boost: 10%
        
        Returns:
            dict with all scores and details
        """
        logger.info(f"Scoring page {page_id}: {url}")
        
        result = {
            'page_id': page_id,
            'url': url,
            'scored_at': datetime.now()
        }
        
        # Check org blocklist first (instant disqualification)
        is_blocked, block_reason = self.check_org_blocklist(domain)
        if is_blocked:
            result['final_composite_score'] = 0
            result['indexable'] = False
            result['blocklist_reason'] = block_reason
            result['components'] = {
                'org_blocked': True,
                'reason': block_reason
            }
            logger.warning(f"Page {page_id} blocked by org blocklist: {block_reason}")
            return result
        
        # Calculate all components
        components = {}
        
        # 1. Islamic Alignment (30%)
        islamic_score, islamic_details = self.calculate_islamic_alignment(content, domain)
        # Normalize from -100:+100 to 0:100
        islamic_normalized = (islamic_score + 100) / 2
        components['islamic_alignment'] = {
            'raw_score': islamic_score,
            'normalized_score': islamic_normalized,
            'details': islamic_details
        }
        
        # 2. Quality Score (25%)
        quality_score, quality_details = self.calculate_quality_score(content, domain, title)
        
        # Add freshness component
        if crawled_at:
            age = datetime.now() - crawled_at
            if age.days < 30:
                freshness = 15
            elif age.days < 90:
                freshness = 10
            elif age.days < 365:
                freshness = 5
            else:
                freshness = 2
            quality_score = min(100, quality_score + freshness)
            quality_details['freshness'] = freshness
        
        components['quality'] = {
            'score': quality_score,
            'details': quality_details
        }
        
        # 3. Authority Score (20%)
        authority_score, authority_details = self.calculate_authority_score(url, domain)
        components['authority'] = {
            'score': authority_score,
            'details': authority_details
        }
        
        # 4. Media Literacy (15%)
        media_score, media_details = self.calculate_media_literacy_score(content, domain, title)
        components['media_literacy'] = {
            'score': media_score,
            'details': media_details
        }
        
        # 5. Equity Boost (10%)
        equity_boost, equity_details = self.calculate_equity_boost(domain)
        components['equity_boost'] = {
            'boost': equity_boost,
            'details': equity_details
        }
        
        # Calculate weighted composite score
        composite = (
            islamic_normalized * 0.30 +
            quality_score * 0.25 +
            authority_score * 0.20 +
            media_score * 0.15 +
            equity_boost * 0.10
        )
        
        # Round to integer
        final_score = int(round(composite))
        
        # Determine indexability
        indexable = final_score >= 25
        
        result['islamic_alignment_score'] = int(islamic_score)
        result['quality_score'] = quality_score
        result['authority_score'] = authority_score
        result['media_literacy_score'] = media_score
        result['equity_boost'] = equity_boost
        result['final_composite_score'] = final_score
        result['indexable'] = indexable
        result['blocklist_reason'] = None
        result['components'] = components
        
        logger.info(f"Page {page_id} final score: {final_score} (indexable={indexable})")
        logger.debug(f"  Islamic: {islamic_score}, Quality: {quality_score}, "
                    f"Authority: {authority_score}, Equity: +{equity_boost}")
        
        return result
    
    # ========================================================================
    # DATABASE PERSISTENCE
    # ========================================================================
    
    def save_scores_to_db(self, result):
        """
        Save scoring results to database
        Updates pages table and logs to page_scoring_logs
        """
        cursor = self.db_conn.cursor()
        try:
            # Update pages table
            cursor.execute("""
                UPDATE pages
                SET islamic_alignment_score = %s,
                    quality_score = %s,
                    authority_score = %s,
                    media_literacy_score = %s,
                    equity_boost = %s,
                    final_composite_score = %s,
                    indexable = %s,
                    scored_at = %s
                WHERE id = %s
            """, (
                result['islamic_alignment_score'],
                result['quality_score'],
                result['authority_score'],
                result['media_literacy_score'],
                result['equity_boost'],
                result['final_composite_score'],
                result['indexable'],
                result['scored_at'],
                result['page_id']
            ))
            
            # Log to page_scoring_logs for audit trail
            components = result['components']
            
            cursor.execute("""
                INSERT INTO page_scoring_logs (
                    page_id, url,
                    islamic_alignment_score, islamic_themes_matched,
                    quality_score, quality_details,
                    authority_score,
                    equity_boost,
                    media_literacy_score,
                    final_composite_score,
                    indexable,
                    blocklist_reason,
                    scored_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                result['page_id'],
                result['url'],
                result['islamic_alignment_score'],
                json.dumps(components.get('islamic_alignment', {}).get('details', {})),
                result['quality_score'],
                json.dumps(components.get('quality', {}).get('details', {})),
                result['authority_score'],
                result['equity_boost'],
                result['media_literacy_score'],
                result['final_composite_score'],
                result['indexable'],
                result.get('blocklist_reason'),
                result['scored_at']
            ))
            
            self.db_conn.commit()
            logger.info(f"Scores saved for page {result['page_id']}")
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error saving scores to database: {e}")
            raise
        finally:
            cursor.close()
    
    def score_page(self, page_id, url, title, content, domain, crawled_at=None):
        """
        Main entry point: Score a page and save to database
        
        Args:
            page_id: Database page ID
            url: Page URL
            title: Page title
            content: Page text content
            domain: Domain name
            crawled_at: When page was crawled (datetime)
        
        Returns:
            Final composite score (0-100)
        """
        result = self.calculate_composite_score(
            page_id, url, title, content, domain, crawled_at
        )
        
        self.save_scores_to_db(result)
        
        return result['final_composite_score']


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def score_page_by_id(db_conn, page_id):
    """
    Score a page by its database ID
    Convenience function that fetches page data and scores it
    """
    cursor = db_conn.cursor()
    try:
        cursor.execute("""
            SELECT id, url, domain, title, content, crawled_at
            FROM pages
            WHERE id = %s
        """, (page_id,))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError(f"Page {page_id} not found")
        
        page_id, url, domain, title, content, crawled_at = result
        
        scorer = CompositeScorer(db_conn)
        final_score = scorer.score_page(
            page_id, url, title, content, domain, crawled_at
        )
        
        return final_score
        
    finally:
        cursor.close()


def rescore_all_pages(db_conn, limit=None):
    """
    Rescore all pages in database
    Useful for applying updated scoring algorithms
    
    Args:
        db_conn: Database connection
        limit: Max number of pages to rescore (None = all)
    """
    cursor = db_conn.cursor()
    try:
        query = "SELECT id FROM pages WHERE content IS NOT NULL"
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        page_ids = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Rescoring {len(page_ids)} pages...")
        
        scorer = CompositeScorer(db_conn)
        
        for i, page_id in enumerate(page_ids, 1):
            try:
                score_page_by_id(db_conn, page_id)
                
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(page_ids)} pages scored")
                    
            except Exception as e:
                logger.error(f"Error scoring page {page_id}: {e}")
                continue
        
        logger.info(f"Rescoring complete: {len(page_ids)} pages processed")
        
    finally:
        cursor.close()


# ============================================================================
# MAIN / TESTING
# ============================================================================

if __name__ == '__main__':
    import os
    import psycopg2
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get database connection
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("âŒ ERROR: DATABASE_URL not set")
        exit(1)
    
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    
    conn = psycopg2.connect(database_url)
    
    print("="*60)
    print("COMPOSITE SCORER TEST")
    print("="*60)
    
    # Test scoring a few pages
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, url, domain
        FROM pages
        WHERE content IS NOT NULL
        LIMIT 5
    """)
    
    test_pages = cursor.fetchall()
    cursor.close()
    
    print(f"\nScoring {len(test_pages)} test pages...\n")
    
    for page_id, url, domain in test_pages:
        try:
            score = score_page_by_id(conn, page_id)
            print(f"âœ… {domain}: {score}/100")
        except Exception as e:
            print(f"âŒ {domain}: Error - {e}")
    
    conn.close()
    
    print("\n" + "="*60)
    print("âœ… Test complete")
    print("="*60)
