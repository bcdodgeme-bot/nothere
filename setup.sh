#!/bin/bash
# NotHere.one Crawler - Quick Setup Script

echo "🔍 NotHere.one Crawler Setup"
echo "================================"
echo ""

# Check Python version
echo "Checking Python version..."
python3 --version || { echo "❌ Python 3 not found"; exit 1; }
echo "✓ Python 3 found"
echo ""

# Install dependencies
echo "Installing dependencies..."
pip3 install -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Check environment variables
echo "Checking environment variables..."

if [ -z "$DATABASE_URL" ]; then
    echo "⚠️  DATABASE_URL not set"
    echo "   Set it with: export DATABASE_URL='postgresql://user:pass@host:port/dbname'"
else
    echo "✓ DATABASE_URL is set"
fi

if [ -z "$REDIS_URL" ]; then
    echo "⚠️  REDIS_URL not set"
    echo "   Set it with: export REDIS_URL='redis://host:port'"
    echo "   Or it will default to: redis://localhost:6379"
else
    echo "✓ REDIS_URL is set"
fi
echo ""

# Initialize database
echo "Database setup:"
echo "Run the following to initialize your database:"
echo "  psql \$DATABASE_URL -f schema.sql"
echo ""

# Make scripts executable
chmod +x test_crawler.py
echo "✓ Made test script executable"
echo ""

# Run tests
echo "Running tests..."
python3 test_crawler.py

echo ""
echo "================================"
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. If database tables don't exist, run:"
echo "   psql \$DATABASE_URL -f schema.sql"
echo ""
echo "2. Test the blocklist:"
echo "   python3 blocklist.py"
echo ""
echo "3. Start crawling:"
echo "   python3 crawler.py --seed seed_urls.txt --max-pages 10"
echo ""
echo "4. For help:"
echo "   python3 crawler.py --help"
echo ""