#!/usr/bin/env python3
"""
Quick setup validation script for Phase 3 WebUI
"""

import sys


def check_imports():
    """Check if required packages are installed"""
    print("Checking required packages...")
    try:
        import flask

        print(f"  ✓ Flask {flask.__version__} installed")
    except ImportError:
        print("  ✗ Flask not installed - run: pip install flask")
        return False

    try:
        import psycopg2

        print(f"  ✓ psycopg2 installed")
    except ImportError:
        print("  ✗ psycopg2 not installed - run: pip install psycopg2-binary")
        return False

    return True


def check_database():
    """Check database connection"""
    print("\nChecking database connection...")
    try:
        import psycopg2
        from config import Config

        conn = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
        )

        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"  ✓ Connected to PostgreSQL")
        print(f"    Version: {version[0][:50]}...")

        # Check for tables
        cur.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='public'
            ORDER BY table_name
        """
        )
        tables = cur.fetchall()

        if tables:
            print(f"  ✓ Found {len(tables)} table(s):")
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cur.fetchone()[0]
                print(f"    - {table[0]}: {count:,} rows")
        else:
            print("  ⚠ Warning: No tables found in database")
            print("    You may need to run Phase 2 to populate the database")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        print("\n  Troubleshooting:")
        print("    1. Check if Docker is running: docker ps")
        print("    2. Start PostgreSQL container: cd ../src && bash start_db.sh")
        print("    3. Verify connection settings in config.py")
        return False


def check_files():
    """Check if required files exist"""
    print("\nChecking application files...")
    import os

    files_to_check = [
        "app/__init__.py",
        "app/routes.py",
        "app/templates/base.html",
        "app/templates/index.html",
        "app/static/app.js",
        "app/static/style.css",
        "config.py",
        "run.py",
    ]

    all_exist = True
    for file in files_to_check:
        if os.path.exists(file):
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file} missing")
            all_exist = False

    return all_exist


def main():
    print("=" * 60)
    print("Phase 3 WebUI Setup Validation")
    print("=" * 60)

    checks = [check_imports(), check_files(), check_database()]

    print("\n" + "=" * 60)
    if all(checks):
        print("✓ All checks passed! You're ready to run the Flask app.")
        print("\nTo start the application:")
        print("  cd /Users/rushipatel/Desktop/cs236_project/flask")
        print("  python run.py")
        print("\nThen open: http://localhost:5000")
    else:
        print("✗ Some checks failed. Please fix the issues above.")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    main()
