"""
Tier 4: Media Literacy Scoring for NotHere.one
==============================================
AI-powered pattern detection using OpenRouter API

Cost-optimized approach:
- 90% of pages: Fast keyword check → neutral 50 (no API cost)
- 10% of pages: OpenRouter analysis → scored 0-100

Models:
- Primary: Google Gemini Flash 1.5 (fast, cheap, good)
- Fallback: OpenRouter Auto (intelligent routing)
- Blocked: OpenAI models (prefer Claude/Gemini/open source)
"""

import os
import json
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)


class MediaLiteracyScorer:
    """
    AI-powered media literacy scoring
    Detects misinformation patterns using OpenRouter
    """
    
    def __init__(self, db_conn=None):
        self.db_conn = db_conn
        
        # OpenRouter configuration
        self.api_key = os.getenv('OPENROUTER_API_KEY')
        self.base_url = 'https://openrouter.ai/api/v1/chat/completions'
        
        # Model configuration
        self.primary_model = 'google/gemini-2.5-flash-lite'
        self.fallback_model = 'openrouter/auto'
        self.blocked_models = ['openai/gpt-4o', 'openai/o1', 'openai/o1-mini']
        
        # API settings
        self.max_tokens = 500
        self.temperature = 0.3
        self.timeout = 15
        
        # Statistics
        self.stats = {
            'total_calls': 0,
            'skipped_neutral': 0,
            'analyzed': 0,
            'errors': 0,
            'fallback_used': 0
        }
        
        # Red flag keywords for fast filtering
        self.red_flag_keywords = [
            # Miracle cure / snake oil
            'miracle cure', 'doctors hate', 'one weird trick',
            'scientists don\'t want you to know', 'big pharma',
            'they don\'t want you to', 'shocking truth',
            'government hiding', 'mainstream media lies',
            'breakthrough discovery', 'suppressed information',
            'what they won\'t tell you', 'banned by',
            'secret that', 'industry doesn\'t want',
            'proven to cure', 'guaranteed results',
            
            # Conspiracy indicators
            'new world order', 'illuminati', 'deep state',
            'false flag', 'crisis actors', 'hoax',
            'cover up', 'conspiracy', 'they\'re hiding',
            
            # Specific conspiracy theories
            'flat earth', 'earth is flat', 'globe lie',
            'moon landing fake', 'moon landing hoax', 'never went to moon',
            'chemtrails', 'chem trails', 'spraying chemicals',
            '5g causes', '5g conspiracy', '5g radiation',
            'vaccines cause autism', 'autism from vaccines', 'vaccine injury',
            'anti-vax', 'anti-vaxx', 'vaccine danger', 'vaccine poison',
            'big pharma conspiracy', 'pharmaceutical conspiracy',
            'qanon', 'wwg1wga', 'trust the plan', 'the storm',
            'adrenochrome', 'pizzagate', 'pedophile ring',
            'covid hoax', 'plandemic', 'scamdemic', 'covid fake',
            'coronavirus hoax', 'virus doesn\'t exist',
            'microchip vaccine', 'vaccine tracking', 'bill gates microchip',
            'lizard people', 'reptilians', 'shape shifters',
            'sandy hook hoax', 'parkland hoax', 'shooting hoax',
            'holocaust didn\'t happen', 'holocaust denial', 'holocaust hoax',
            '9/11 inside job', '9/11 controlled demolition', 'twin towers explosives',
            'agenda 21', 'agenda 2030', 'un takeover',
            'jade helm', 'fema camps', 'martial law coming',
            'crisis actor', 'paid protesters', 'soros funded',
            'george soros conspiracy', 'soros controls',
            'rothschild conspiracy', 'banking elite conspiracy',
            'freemasons control', 'satanic ritual', 'satanic panic',
            
            # MLM / Get rich quick
            'be your own boss', 'financial freedom',
            'work from home unlimited', 'passive income guaranteed',
            'join my team', 'ground floor opportunity',
            'unlimited earning potential', 'retired at 30',
            
            # Predatory health claims
            'detox', 'toxins', 'cleanse', 'boost your immune system',
            'natural alternative to', 'big pharma doesn\'t want',
            'FDA doesn\'t approve because', 'alternative to chemotherapy',
            
            # Historical revisionism
            'flat earth', 'moon landing fake', 'holocaust hoax',
            'crisis actors', 'false flag operation',
            
            # Statistical manipulation indicators
            'correlation equals causation', '100% of people',
            'studies show', 'experts agree', 'research proves',
            'science says' # Often misused
        ]
        
        if not self.api_key:
            logger.warning("OPENROUTER_API_KEY not set - media literacy will return neutral scores")
    
    def needs_analysis(self, content, domain):
        """
        Fast keyword check to determine if content needs AI analysis
        
        Returns:
            tuple: (needs_analysis: bool, matched_keywords: list)
        """
        if not content or len(content) < 100:
            return False, []
        
        content_lower = content.lower()
        matched = []
        
        for keyword in self.red_flag_keywords:
            if keyword in content_lower:
                matched.append(keyword)
        
        # If 2+ red flag keywords, send to AI
        needs_ai = len(matched) >= 2
        
        if needs_ai:
            logger.info(f"Red flags detected ({len(matched)}): {domain[:50]}")
            logger.debug(f"  Matched keywords: {matched[:5]}")
        
        return needs_ai, matched
    
    def _build_analysis_prompt(self, content, domain, title):
        """
        Build comprehensive prompt for pattern detection
        
        Returns:
            str: Formatted prompt
        """
        # Truncate content to first 2500 characters for cost efficiency
        content_sample = content[:2500] if content else ""
        
        prompt = f"""Analyze this webpage content for media literacy red flags.

CONTENT:
Title: {title or 'No title'}
Domain: {domain}
Text: {content_sample}

Detect these 7 patterns:
1. Scientific Consensus Mismatch - contradicts established scientific consensus
2. Extraordinary Claims - miracle cures, one weird trick, extreme promises without evidence
3. Statistical Manipulation - correlation as causation, cherry-picked data, misleading stats
4. Source-Expertise Mismatch - unqualified author making expert claims
5. Conflict of Interest - undisclosed sponsorships, selling promoted products
6. Historical Revisionism - contradicts established historical record
7. Predatory Economic - MLM recruitment, pressure tactics, get-rich-quick schemes

IMPORTANT NUANCE:
- News reporting violence ≠ glorifying violence
- Academic discussion ≠ advocacy
- Historical analysis ≠ revisionism
- Medical information from qualified sources ≠ miracle cure claims
- Educational content explaining conspiracies ≠ promoting them

Return ONLY a JSON object (no markdown, no code blocks):
{{
  "major_red_flags": ["pattern_name1", "pattern_name2"],
  "minor_concerns": ["pattern_name3"],
  "explanation": "Brief 1-2 sentence reasoning",
  "credibility_score": 0-100,
  "context_box_needed": true or false,
  "context_box_text": "Educational context for users if needed"
}}

Pattern names: scientific_mismatch, extraordinary_claims, statistical_manipulation, expertise_mismatch, conflict_of_interest, historical_revisionism, predatory_economic

Be strict but fair. Academic and educational content should score 70+. Genuine misinformation should score 0-40."""

        return prompt
    
    def _call_openrouter(self, prompt, model, attempt=1):
        """
        Make API call to OpenRouter
        
        Returns:
            dict: API response or None on error
        """
        if not self.api_key:
            logger.error("No OpenRouter API key configured")
            return None
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://nothere.one',
            'X-Title': 'NotHere.one Media Literacy Scorer'
        }
        
        data = {
            'model': model,
            'messages': [
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'max_tokens': self.max_tokens,
            'temperature': 0.3,
            'route': 'fallback'  # Enable automatic fallback
        }
        
        # Block OpenAI models
        if self.blocked_models:
            data['models'] = {
                'blacklist': self.blocked_models
            }
        
        try:
            logger.debug(f"OpenRouter API call (model={model}, attempt={attempt})")
            
            response = requests.post(
                self.base_url,
                headers=headers,
                json=data,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            result = response.json()
            
            # Log usage
            if 'usage' in result:
                usage = result['usage']
                logger.debug(f"  Tokens: {usage.get('prompt_tokens', 0)} prompt, "
                           f"{usage.get('completion_tokens', 0)} completion")
            
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"OpenRouter timeout (attempt {attempt})")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API error: {e}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error calling OpenRouter: {e}")
            return None
    
    def analyze_with_openrouter(self, content, domain, title):
        """
        Send content to OpenRouter for pattern analysis
        
        Returns:
            tuple: (score: int, details: dict)
        """
        # Build prompt
        prompt = self._build_analysis_prompt(content, domain, title)
        
        # Try primary model
        response = self._call_openrouter(prompt, self.primary_model, attempt=1)
        
        # Fallback to auto if primary fails
        if response is None:
            logger.warning(f"Primary model failed, trying fallback: {self.fallback_model}")
            response = self._call_openrouter(prompt, self.fallback_model, attempt=2)
            self.stats['fallback_used'] += 1
        
        # If both fail, return neutral
        if response is None:
            logger.error("Both primary and fallback models failed")
            self.stats['errors'] += 1
            return 50, {
                'status': 'error',
                'error': 'API unavailable',
                'fallback_reason': 'Both models failed'
            }
        
        # Extract response
        try:
            choices = response.get('choices', [])
            if not choices:
                raise ValueError("No choices in response")
            
            message = choices[0].get('message', {})
            content_text = message.get('content', '').strip()
            
            # Remove markdown code blocks if present
            if content_text.startswith('```'):
                content_text = content_text.split('```')[1]
                if content_text.startswith('json'):
                    content_text = content_text[4:]
                content_text = content_text.strip()
            
            # Parse JSON
            analysis = json.loads(content_text)
            
            # Extract score
            score = analysis.get('credibility_score', 50)
            
            # Validate score range
            if not (0 <= score <= 100):
                logger.warning(f"Invalid score {score}, clamping to 0-100")
                score = max(0, min(100, score))
            
            # Build details
            details = {
                'status': 'analyzed',
                'model_used': response.get('model', self.primary_model),
                'major_red_flags': analysis.get('major_red_flags', []),
                'minor_concerns': analysis.get('minor_concerns', []),
                'explanation': analysis.get('explanation', ''),
                'context_box_needed': analysis.get('context_box_needed', False),
                'context_box_text': analysis.get('context_box_text', ''),
                'raw_score': score
            }
            
            self.stats['analyzed'] += 1
            
            # Log significant findings
            if score < 40:
                logger.warning(f"Low credibility score: {score}/100")
                logger.warning(f"  Red flags: {details['major_red_flags']}")
            elif score >= 80:
                logger.info(f"High credibility score: {score}/100")
            
            return int(score), details
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Response content: {content_text[:500]}")
            self.stats['errors'] += 1
            return 50, {
                'status': 'error',
                'error': 'Invalid JSON response',
                'raw_response': content_text[:200]
            }
            
        except Exception as e:
            logger.error(f"Error processing OpenRouter response: {e}")
            self.stats['errors'] += 1
            return 50, {
                'status': 'error',
                'error': str(e)
            }
    
    def calculate_media_literacy_score(self, content, domain, title=None):
        """
        Main entry point for media literacy scoring
        
        Cost-optimized: Only analyzes suspicious content
        
        Returns:
            tuple: (score: int, details: dict)
        """
        self.stats['total_calls'] += 1
        
        # Fast keyword check
        needs_ai, matched_keywords = self.needs_analysis(content, domain)
        
        if not needs_ai:
            # No red flags - return neutral score (no cost)
            self.stats['skipped_neutral'] += 1
            return 50, {
                'status': 'neutral',
                'reason': 'No red flag keywords detected',
                'skipped_analysis': True
            }
        
        # Red flags detected - send to AI
        logger.info(f"Sending to OpenRouter: {domain[:50]}")
        
        score, details = self.analyze_with_openrouter(content, domain, title)
        
        # Add matched keywords to details
        details['triggered_by'] = matched_keywords[:10]  # Top 10
        
        return score, details
    
    def get_stats(self):
        """Get scorer statistics"""
        skip_rate = (self.stats['skipped_neutral'] / self.stats['total_calls'] * 100 
                     if self.stats['total_calls'] > 0 else 0)
        
        return {
            **self.stats,
            'skip_rate_percent': round(skip_rate, 1)
        }


# Standalone function for use in composite_scorer.py
_scorer_instance = None

def calculate_media_literacy_score(content, domain, title=None, db_conn=None):
    """
    Calculate media literacy score (standalone function)
    
    This is the function called by composite_scorer.py
    
    Returns:
        tuple: (score: int, details: dict)
    """
    global _scorer_instance
    if _scorer_instance is None:
        _scorer_instance = MediaLiteracyScorer(db_conn)
    elif db_conn and not _scorer_instance.db_conn:
        _scorer_instance.db_conn = db_conn
    
    return _scorer_instance.calculate_media_literacy_score(content, domain, title)
