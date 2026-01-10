"""
NotHere.one - Ethical Search Engine
====================================
Flask web application with full search, feedback, and media literacy features.

Run locally: python app.py
Deploy: Railway web service
"""

import os
import json
import time
import uuid
import logging
from datetime import datetime
from functools import wraps

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from flask import (
    Flask, render_template, request, jsonify, 
    make_response, redirect, url_for, g
)

# ============================================================================
# CONFIGURATION
# ============================================================================

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'nothere-dev-key-change-in-production')

# Database
DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# OpenRouter API for media literacy
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY', '')
OPENROUTER_URL = 'https://openrouter.ai/api/v1/chat/completions'
AI_MODEL = 'google/gemini-flash-1.5'

# Search settings
RESULTS_PER_PAGE = 10
MIN_INDEXABLE_SCORE = 25
MAX_QUERY_LENGTH = 300

# Media literacy trigger keywords
MEDIA_LITERACY_TRIGGERS = [
    'vaccine', 'autism', 'ivermectin', 'hydroxychloroquine',
    'election fraud', 'stolen election', 'deep state',
    'chemtrails', 'flat earth', '5g', 'microchip',
    'fluoride', 'gmo', 'monsanto', 'big pharma',
    'climate hoax', 'global warming hoax',
    'miracle cure', 'doctors hate', 'they dont want you to know',
    'suppressed', 'censored truth', 'exposed',
    'plandemic', 'scamdemic', 'bioweapon'
]

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db():
    """Get database connection for current request"""
    if 'db' not in g:
        g.db = psycopg2.connect(DATABASE_URL)
    return g.db

@app.teardown_appcontext
def close_db(exception):
    """Close database connection at end of request"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    """Execute a query and return results as dictionaries"""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, args)
    results = cursor.fetchall()
    cursor.close()
    return (results[0] if results else None) if one else results

def execute_db(query, args=()):
    """Execute a query that modifies data"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(query, args)
    conn.commit()
    cursor.close()

# ============================================================================
# SESSION MANAGEMENT
# ============================================================================

def get_or_create_session():
    """Get existing session from cookie or create new one"""
    session_id = request.cookies.get('nothere_session')
    
    if session_id:
        # Check if session exists in database
        session = query_db(
            "SELECT * FROM user_sessions WHERE session_id = %s",
            (session_id,),
            one=True
        )
        if session:
            # Update last active
            execute_db(
                "UPDATE user_sessions SET last_active = NOW() WHERE session_id = %s",
                (session_id,)
            )
            return dict(session)
    
    # Create new session
    session_id = str(uuid.uuid4())
    execute_db(
        """INSERT INTO user_sessions (session_id, total_points, quests_completed, created_at, last_active)
           VALUES (%s, 0, 0, NOW(), NOW())""",
        (session_id,)
    )
    
    return {
        'session_id': session_id,
        'total_points': 0,
        'quests_completed': 0,
        'is_new': True
    }

def set_session_cookie(response, session_id):
    """Set session cookie on response"""
    response.set_cookie(
        'nothere_session',
        session_id,
        max_age=365*24*60*60,  # 1 year
        httponly=True,
        samesite='Lax'
    )
    return response

# ============================================================================
# SEARCH FUNCTIONS
# ============================================================================

def search_pages(query, sort='relevance', page=1):
    """
    Search indexed pages
    
    Args:
        query: Search string
        sort: 'relevance', 'score', or 'recent'
        page: Page number (1-indexed)
    
    Returns:
        dict with results, total_count, and timing
    """
    start_time = time.time()
    offset = (page - 1) * RESULTS_PER_PAGE
    
    # Build ORDER BY clause based on sort
    if sort == 'score':
        order_by = "final_composite_score DESC, similarity(title, %s) DESC"
    elif sort == 'recent':
        order_by = "crawled_at DESC, similarity(title, %s) DESC"
    else:  # relevance (default)
        order_by = """
            (similarity(title, %s) * 2 + similarity(content, %s)) DESC,
            final_composite_score DESC
        """
    
    # Count total results
    count_result = query_db(
        """
        SELECT COUNT(*) as total
        FROM pages
        WHERE indexable = true
          AND final_composite_score >= %s
          AND (
              title % %s 
              OR content % %s
              OR title ILIKE %s
              OR content ILIKE %s
          )
        """,
        (MIN_INDEXABLE_SCORE, query, query, f'%{query}%', f'%{query}%'),
        one=True
    )
    total_count = count_result['total'] if count_result else 0
    
    # Get results with relevance scoring
    if sort == 'relevance':
        results = query_db(
            f"""
            SELECT 
                p.id,
                p.url,
                p.domain,
                p.title,
                LEFT(p.content, 300) as snippet,
                p.final_composite_score,
                p.equity_boost,
                p.crawled_at,
                similarity(p.title, %s) as title_sim,
                similarity(p.content, %s) as content_sim,
                ed.minority_owned,
                ed.women_owned,
                ed.veteran_owned,
                ed.b_corp,
                ed.lgbtq_owned,
                ed.disability_owned,
                COALESCE(pfs.helpful_count, 0) as helpful_count,
                COALESCE(pfs.not_helpful_count, 0) as not_helpful_count
            FROM pages p
            LEFT JOIN equity_domains ed ON p.domain = ed.domain
            LEFT JOIN page_feedback_summary pfs ON p.id = pfs.page_id
            WHERE p.indexable = true
              AND p.final_composite_score >= %s
              AND (
                  p.title % %s 
                  OR p.content % %s
                  OR p.title ILIKE %s
                  OR p.content ILIKE %s
              )
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
            """,
            (query, query, MIN_INDEXABLE_SCORE, query, query, f'%{query}%', f'%{query}%',
             query, query, RESULTS_PER_PAGE, offset)
        )
    else:
        results = query_db(
            f"""
            SELECT 
                p.id,
                p.url,
                p.domain,
                p.title,
                LEFT(p.content, 300) as snippet,
                p.final_composite_score,
                p.equity_boost,
                p.crawled_at,
                similarity(p.title, %s) as title_sim,
                similarity(p.content, %s) as content_sim,
                ed.minority_owned,
                ed.women_owned,
                ed.veteran_owned,
                ed.b_corp,
                ed.lgbtq_owned,
                ed.disability_owned,
                COALESCE(pfs.helpful_count, 0) as helpful_count,
                COALESCE(pfs.not_helpful_count, 0) as not_helpful_count
            FROM pages p
            LEFT JOIN equity_domains ed ON p.domain = ed.domain
            LEFT JOIN page_feedback_summary pfs ON p.id = pfs.page_id
            WHERE p.indexable = true
              AND p.final_composite_score >= %s
              AND (
                  p.title % %s 
                  OR p.content % %s
                  OR p.title ILIKE %s
                  OR p.content ILIKE %s
              )
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
            """,
            (query, query, MIN_INDEXABLE_SCORE, query, query, f'%{query}%', f'%{query}%',
             query, RESULTS_PER_PAGE, offset)
        )
    
    # Calculate timing
    search_time_ms = int((time.time() - start_time) * 1000)
    
    # Log search (anonymized)
    try:
        execute_db(
            """INSERT INTO search_logs (query, results_count, top_result_score, search_time_ms, sort_type, page_number)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (query, total_count, 
             results[0]['final_composite_score'] if results else None,
             search_time_ms, sort, page)
        )
    except Exception as e:
        logger.warning(f"Failed to log search: {e}")
    
    return {
        'results': [dict(r) for r in results],
        'total_count': total_count,
        'search_time_ms': search_time_ms,
        'page': page,
        'total_pages': (total_count + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
    }

def get_score_badge(score):
    """Return badge emoji and class based on score"""
    if score >= 50:
        return {'emoji': '⭐', 'class': 'badge-high', 'label': 'High Quality'}
    elif score >= 40:
        return {'emoji': '🟢', 'class': 'badge-medium', 'label': 'Good'}
    else:
        return {'emoji': '🟡', 'class': 'badge-low', 'label': 'Fair'}

def get_equity_badges(result):
    """Return list of equity badges for a result"""
    badges = []
    if result.get('minority_owned'):
        badges.append({'emoji': '✊', 'label': 'Minority-Owned'})
    if result.get('women_owned'):
        badges.append({'emoji': '👩', 'label': 'Women-Owned'})
    if result.get('veteran_owned'):
        badges.append({'emoji': '🎖️', 'label': 'Veteran-Owned'})
    if result.get('b_corp'):
        badges.append({'emoji': '🌍', 'label': 'B-Corp Certified'})
    if result.get('lgbtq_owned'):
        badges.append({'emoji': '🏳️‍🌈', 'label': 'LGBTQ+-Owned'})
    if result.get('disability_owned'):
        badges.append({'emoji': '♿', 'label': 'Disability-Owned'})
    return badges

# ============================================================================
# MEDIA LITERACY FUNCTIONS
# ============================================================================

def check_media_literacy_triggers(content):
    """Check if content contains trigger keywords"""
    content_lower = content.lower()
    found_triggers = []
    for trigger in MEDIA_LITERACY_TRIGGERS:
        if trigger in content_lower:
            found_triggers.append(trigger)
    return found_triggers

def get_or_create_media_literacy_warning(page_id, title, content, domain):
    """
    Check for existing warning or analyze with AI
    Returns warning dict or None
    """
    # Check cache first
    existing = query_db(
        "SELECT * FROM media_literacy_warnings WHERE page_id = %s",
        (page_id,),
        one=True
    )
    
    if existing:
        if existing['has_warning']:
            return dict(existing)
        return None  # Already checked, no warning needed
    
    # Check for trigger keywords
    triggers = check_media_literacy_triggers(f"{title} {content}")
    
    if not triggers:
        # No triggers, cache as no-warning
        execute_db(
            """INSERT INTO media_literacy_warnings (page_id, has_warning, checked_at)
               VALUES (%s, FALSE, NOW())
               ON CONFLICT (page_id) DO NOTHING""",
            (page_id,)
        )
        return None
    
    # Has triggers - analyze with AI
    warning = analyze_with_ai(page_id, title, content, domain, triggers)
    return warning

def analyze_with_ai(page_id, title, content, domain, triggers):
    """Use OpenRouter to analyze page for misinformation"""
    if not OPENROUTER_API_KEY:
        logger.warning("OPENROUTER_API_KEY not set, skipping AI analysis")
        return None
    
    # Truncate content for API
    content_excerpt = content[:2000] if content else ""
    
    prompt = f"""You are a fact-checking assistant. Analyze this webpage for disputed or misleading claims.

Title: {title}
Domain: {domain}
Trigger keywords found: {', '.join(triggers)}
Content excerpt: {content_excerpt}

Does this page make claims that conflict with:
- Scientific consensus (medicine, climate, etc.)
- Peer-reviewed research
- Credible institutions (CDC, WHO, NASA, etc.)
- Established historical facts

Respond in JSON format:
{{
    "has_disputed_claims": true/false,
    "confidence": 0.0-1.0,
    "claim_summary": "Brief description of what the page claims",
    "counter_summary": "What the scientific evidence actually shows",
    "counter_sources": [
        {{"name": "Source name", "finding": "What they found", "url": "URL if known"}}
    ]
}}

If the page is factual and not misleading, set has_disputed_claims to false."""

    try:
        response = requests.post(
            OPENROUTER_URL,
            headers={
                'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                'Content-Type': 'application/json',
                'HTTP-Referer': 'https://nothere.one',
                'X-Title': 'NotHere.one Media Literacy'
            },
            json={
                'model': AI_MODEL,
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 500,
                'temperature': 0.3
            },
            timeout=15
        )
        
        response.raise_for_status()
        result = response.json()
        
        # Parse AI response
        ai_text = result['choices'][0]['message']['content']
        
        # Extract JSON from response (handle markdown code blocks)
        if '```json' in ai_text:
            ai_text = ai_text.split('```json')[1].split('```')[0]
        elif '```' in ai_text:
            ai_text = ai_text.split('```')[1].split('```')[0]
        
        ai_data = json.loads(ai_text.strip())
        
        if not ai_data.get('has_disputed_claims'):
            # No issues found
            execute_db(
                """INSERT INTO media_literacy_warnings 
                   (page_id, has_warning, trigger_keywords, ai_model, checked_at)
                   VALUES (%s, FALSE, %s, %s, NOW())
                   ON CONFLICT (page_id) DO NOTHING""",
                (page_id, triggers, AI_MODEL)
            )
            return None
        
        # Has warning - save to database
        import random
        warning_style = random.choice(['side_quest', 'hold_up'])
        
        execute_db(
            """INSERT INTO media_literacy_warnings 
               (page_id, has_warning, warning_style, claim_summary, counter_summary, 
                counter_sources, trigger_keywords, ai_model, confidence_score, checked_at)
               VALUES (%s, TRUE, %s, %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (page_id) DO UPDATE SET
                   has_warning = TRUE,
                   warning_style = EXCLUDED.warning_style,
                   claim_summary = EXCLUDED.claim_summary,
                   counter_summary = EXCLUDED.counter_summary,
                   counter_sources = EXCLUDED.counter_sources,
                   checked_at = NOW()""",
            (page_id, warning_style, 
             ai_data.get('claim_summary', ''),
             ai_data.get('counter_summary', ''),
             json.dumps(ai_data.get('counter_sources', [])),
             triggers, AI_MODEL, ai_data.get('confidence', 0.8))
        )
        
        return {
            'page_id': page_id,
            'has_warning': True,
            'warning_style': warning_style,
            'claim_summary': ai_data.get('claim_summary', ''),
            'counter_summary': ai_data.get('counter_summary', ''),
            'counter_sources': ai_data.get('counter_sources', [])
        }
        
    except Exception as e:
        logger.error(f"AI analysis failed for page {page_id}: {e}")
        return None

# ============================================================================
# STATS FUNCTIONS
# ============================================================================

def get_index_stats():
    """Get statistics about the search index"""
    stats = query_db(
        """
        SELECT 
            COUNT(*) as total_pages,
            COUNT(*) FILTER (WHERE indexable = true) as indexable_pages,
            COUNT(*) FILTER (WHERE final_composite_score >= 50) as high_quality,
            COUNT(DISTINCT domain) as unique_domains,
            MAX(crawled_at) as last_crawled
        FROM pages
        """,
        one=True
    )
    return dict(stats) if stats else {}

def get_leaderboard(limit=10):
    """Get top users by Internet Points"""
    results = query_db(
        """
        SELECT 
            session_id,
            total_points,
            quests_completed,
            RANK() OVER (ORDER BY total_points DESC) as rank
        FROM user_sessions
        WHERE total_points > 0
        ORDER BY total_points DESC
        LIMIT %s
        """,
        (limit,)
    )
    return [dict(r) for r in results]

# ============================================================================
# ROUTES - PAGES
# ============================================================================

@app.route('/')
def index():
    """Homepage with search box"""
    session = get_or_create_session()
    stats = get_index_stats()
    
    response = make_response(render_template(
        'index.html',
        stats=stats,
        session=session
    ))
    
    return set_session_cookie(response, session['session_id'])

@app.route('/search')
def search():
    """Search results page"""
    session = get_or_create_session()
    
    # Get search parameters
    query = request.args.get('q', '').strip()
    sort = request.args.get('sort', 'relevance')
    page = request.args.get('page', 1, type=int)
    
    # Validate
    if not query:
        response = make_response(render_template(
            'results.html',
            query='',
            error="Looks like you didn't type anything!",
            results=[],
            session=session
        ))
        return set_session_cookie(response, session['session_id'])
    
    if len(query) > MAX_QUERY_LENGTH:
        query = query[:MAX_QUERY_LENGTH]
    
    if sort not in ['relevance', 'score', 'recent']:
        sort = 'relevance'
    
    if page < 1:
        page = 1
    
    # Update session search count
    execute_db(
        "UPDATE user_sessions SET searches_performed = searches_performed + 1 WHERE session_id = %s",
        (session['session_id'],)
    )
    
    # Perform search
    search_results = search_pages(query, sort, page)
    
    # Add badges and check media literacy for each result
    for result in search_results['results']:
        result['score_badge'] = get_score_badge(result['final_composite_score'])
        result['equity_badges'] = get_equity_badges(result)
        
        # Check media literacy (async would be better but keeping it simple)
        triggers = check_media_literacy_triggers(f"{result['title']} {result['snippet']}")
        if triggers:
            warning = get_or_create_media_literacy_warning(
                result['id'], 
                result['title'], 
                result['snippet'], 
                result['domain']
            )
            result['media_literacy_warning'] = warning
    
    response = make_response(render_template(
        'results.html',
        query=query,
        sort=sort,
        results=search_results['results'],
        total_count=search_results['total_count'],
        search_time_ms=search_results['search_time_ms'],
        page=page,
        total_pages=search_results['total_pages'],
        session=session
    ))
    
    return set_session_cookie(response, session['session_id'])

@app.route('/privacy')
def privacy():
    """Privacy policy page"""
    session = get_or_create_session()
    
    response = make_response(render_template(
        'privacy.html',
        session=session
    ))
    
    return set_session_cookie(response, session['session_id'])

@app.route('/leaderboard')
def leaderboard():
    """Leaderboard page"""
    session = get_or_create_session()
    leaders = get_leaderboard(100)
    
    # Find current user's rank
    user_rank = None
    for leader in leaders:
        if leader['session_id'] == session['session_id']:
            user_rank = leader['rank']
            break
    
    # If not in top 100, get their rank
    if user_rank is None and session['total_points'] > 0:
        rank_result = query_db(
            """
            SELECT COUNT(*) + 1 as rank
            FROM user_sessions
            WHERE total_points > %s
            """,
            (session['total_points'],),
            one=True
        )
        user_rank = rank_result['rank'] if rank_result else None
    
    response = make_response(render_template(
        'leaderboard.html',
        leaders=leaders,
        session=session,
        user_rank=user_rank
    ))
    
    return set_session_cookie(response, session['session_id'])

# ============================================================================
# ROUTES - API ENDPOINTS
# ============================================================================

@app.route('/api/feedback', methods=['POST'])
def submit_feedback():
    """Submit helpful/not helpful feedback for a result"""
    session = get_or_create_session()
    
    data = request.get_json()
    page_id = data.get('page_id')
    feedback_type = data.get('feedback_type')
    search_query = data.get('query', '')
    
    if not page_id or feedback_type not in ['helpful', 'not_helpful']:
        return jsonify({'error': 'Invalid feedback'}), 400
    
    try:
        execute_db(
            """INSERT INTO page_feedback (page_id, session_id, feedback_type, search_query)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (page_id, session_id) 
               DO UPDATE SET feedback_type = EXCLUDED.feedback_type""",
            (page_id, session['session_id'], feedback_type, search_query)
        )
        
        return jsonify({'success': True})
        
    except Exception as e:
        logger.error(f"Feedback error: {e}")
        return jsonify({'error': 'Failed to save feedback'}), 500

@app.route('/api/media-literacy/claim', methods=['POST'])
def claim_media_literacy_points():
    """Claim Internet Points for engaging with media literacy warning"""
    session = get_or_create_session()
    
    data = request.get_json()
    page_id = data.get('page_id')
    warning_id = data.get('warning_id')
    
    if not page_id:
        return jsonify({'error': 'Invalid request'}), 400
    
    # Check if already claimed
    existing = query_db(
        """SELECT * FROM media_literacy_interactions 
           WHERE session_id = %s AND page_id = %s""",
        (session['session_id'], page_id),
        one=True
    )
    
    if existing:
        return jsonify({
            'success': False,
            'message': 'Already claimed',
            'total_points': session['total_points']
        })
    
    # Award points
    points_to_award = 5
    
    try:
        # Record interaction
        execute_db(
            """INSERT INTO media_literacy_interactions 
               (session_id, page_id, warning_id, points_awarded)
               VALUES (%s, %s, %s, %s)""",
            (session['session_id'], page_id, warning_id, points_to_award)
        )
        
        # Update session points
        execute_db(
            """UPDATE user_sessions 
               SET total_points = total_points + %s,
                   quests_completed = quests_completed + 1
               WHERE session_id = %s""",
            (points_to_award, session['session_id'])
        )
        
        # Get updated total
        updated_session = query_db(
            "SELECT total_points, quests_completed FROM user_sessions WHERE session_id = %s",
            (session['session_id'],),
            one=True
        )
        
        return jsonify({
            'success': True,
            'points_awarded': points_to_award,
            'total_points': updated_session['total_points'],
            'quests_completed': updated_session['quests_completed']
        })
        
    except Exception as e:
        logger.error(f"Points claim error: {e}")
        return jsonify({'error': 'Failed to award points'}), 500

@app.route('/api/stats')
def api_stats():
    """Get index statistics"""
    stats = get_index_stats()
    return jsonify(stats)

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(e):
    session = get_or_create_session()
    response = make_response(render_template('404.html', session=session), 404)
    return set_session_cookie(response, session['session_id'])

@app.errorhandler(500)
def server_error(e):
    session = get_or_create_session()
    response = make_response(render_template('500.html', session=session), 500)
    return set_session_cookie(response, session['session_id'])

# ============================================================================
# TEMPLATE FILTERS
# ============================================================================

@app.template_filter('truncate_url')
def truncate_url(url, max_length=60):
    """Truncate long URLs for display"""
    if len(url) <= max_length:
        return url
    return url[:max_length-3] + '...'

@app.template_filter('time_ago')
def time_ago(dt):
    """Convert datetime to 'X days ago' format"""
    if not dt:
        return 'Unknown'
    
    now = datetime.utcnow()
    diff = now - dt
    
    if diff.days > 365:
        years = diff.days // 365
        return f"{years} year{'s' if years > 1 else ''} ago"
    elif diff.days > 30:
        months = diff.days // 30
        return f"{months} month{'s' if months > 1 else ''} ago"
    elif diff.days > 0:
        return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
    elif diff.seconds > 3600:
        hours = diff.seconds // 3600
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    else:
        return "Just now"

@app.template_filter('format_number')
def format_number(num):
    """Format number with commas"""
    if num is None:
        return '0'
    return f"{num:,}"

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    logger.info(f"Starting NotHere.one on port {port}")
    logger.info(f"Debug mode: {debug}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
