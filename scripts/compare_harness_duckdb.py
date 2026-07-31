#!/usr/bin/env python3
"""Compare query results between HarnessDB and DuckDB."""

import json
import duckdb
import mysql.connector
from pathlib import Path
from datetime import datetime
import re

class ClaudeLogsComparator:
    def __init__(self):
        self.harness_config = {
            'host': '127.0.0.1',
            'port': 9030,
            'user': 'root',
            'database': 'claude_logs'
        }
        self.duckdb_path = '/tmp/claude_logs.duckdb'
        self.claude_dir = Path.home() / '.claude'

    def setup_duckdb(self):
        """Create DuckDB database with same schema and data."""
        print("=== Setting up DuckDB ===")

        # Remove existing database
        Path(self.duckdb_path).unlink(missing_ok=True)

        conn = duckdb.connect(self.duckdb_path)

        # Create tables
        conn.execute("""
            CREATE TABLE command_history (
                id INTEGER PRIMARY KEY,
                display VARCHAR,
                pasted_text VARCHAR,
                timestamp BIGINT,
                project VARCHAR,
                session_id VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE transcripts (
                id INTEGER PRIMARY KEY,
                message_type VARCHAR,
                timestamp_str VARCHAR,
                content VARCHAR,
                session_file VARCHAR
            )
        """)

        # Import history.jsonl
        print("Importing history.jsonl...")
        history_file = self.claude_dir / 'history.jsonl'
        with open(history_file, 'r') as f:
            for i, line in enumerate(f):
                data = json.loads(line.strip())

                pasted_text = ""
                if data.get('pastedContents'):
                    for key, value in data['pastedContents'].items():
                        if value.get('type') == 'text':
                            pasted_text = value.get('content', '')
                            break

                conn.execute("""
                    INSERT INTO command_history (id, display, pasted_text, timestamp, project, session_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    i + 1,
                    data.get('display', ''),
                    pasted_text,
                    data.get('timestamp', 0),
                    data.get('project', ''),
                    data.get('sessionId', '')
                ])

        print(f"  Imported {i+1} rows into command_history")

        # Import transcripts
        print("Importing transcripts...")
        transcripts_dir = self.claude_dir / 'transcripts'
        id_counter = 1

        for jsonl_file in sorted(transcripts_dir.glob('*.jsonl')):
            session_file = jsonl_file.name
            with open(jsonl_file, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        msg_type = data.get('type', '')
                        timestamp_str = data.get('timestamp', '')

                        content_data = data.get('content', '')
                        if isinstance(content_data, list):
                            content = ' '.join(str(x) for x in content_data)
                        else:
                            content = str(content_data)

                        conn.execute("""
                            INSERT INTO transcripts (id, message_type, timestamp_str, content, session_file)
                            VALUES (?, ?, ?, ?, ?)
                        """, [id_counter, msg_type, timestamp_str, content, session_file])

                        id_counter += 1
                    except Exception as e:
                        print(f"  Warning: {e}")
                        continue

        print(f"  Imported {id_counter-1} rows into transcripts")

        conn.close()
        print("DuckDB setup complete\n")

    def query_harness(self, sql):
        """Execute query on HarnessDB."""
        conn = mysql.connector.connect(**self.harness_config)
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            if sql.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                return result
            else:
                return f"Affected rows: {cursor.rowcount}"
        finally:
            cursor.close()
            conn.close()

    def query_duckdb(self, sql):
        """Execute query on DuckDB."""
        conn = duckdb.connect(self.duckdb_path, read_only=True)
        result = conn.execute(sql).fetchall()
        conn.close()
        return result

    def compare_query(self, name, harness_sql, duckdb_sql=None):
        """Run query on both databases and compare results."""
        if duckdb_sql is None:
            duckdb_sql = harness_sql

        print(f"\n=== Query: {name} ===")
        print(f"HarnessDB SQL: {harness_sql}")

        try:
            harness_result = self.query_harness(harness_sql)
            print(f"HarnessDB result: {harness_result}")
        except Exception as e:
            print(f"HarnessDB error: {e}")
            harness_result = None

        try:
            duckdb_result = self.query_duckdb(duckdb_sql)
            print(f"DuckDB result: {duckdb_result}")
        except Exception as e:
            print(f"DuckDB error: {e}")
            duckdb_result = None

        # Compare
        if harness_result and duckdb_result:
            if str(harness_result) == str(duckdb_result):
                print("✅ Results match!")
            else:
                print("❌ Results differ")
        else:
            print("⚠️  Could not compare (one or both queries failed)")

    def run_all_tests(self):
        """Run comprehensive test suite."""
        print("\n" + "="*80)
        print("COMPREHENSIVE COMPARISON TEST SUITE")
        print("="*80)

        # Test 1: Basic count
        self.compare_query(
            "Count command_history",
            "SELECT COUNT(*) FROM command_history"
        )

        # Test 2: Count transcripts
        self.compare_query(
            "Count transcripts",
            "SELECT COUNT(*) FROM transcripts"
        )

        # Test 3: Distinct projects
        self.compare_query(
            "Distinct projects",
            "SELECT COUNT(DISTINCT project) FROM command_history"
        )

        # Test 4: Group by project
        self.compare_query(
            "Commands per project",
            "SELECT project, COUNT(*) as cnt FROM command_history GROUP BY project ORDER BY cnt DESC LIMIT 10"
        )

        # Test 5: Time range analysis
        self.compare_query(
            "Time range (min/max timestamp)",
            "SELECT MIN(timestamp), MAX(timestamp) FROM command_history"
        )

        # Test 6: Session analysis
        self.compare_query(
            "Sessions per project",
            "SELECT project, COUNT(DISTINCT session_id) as session_count FROM command_history GROUP BY project ORDER BY session_count DESC LIMIT 5"
        )

        # Test 7: Message type distribution
        self.compare_query(
            "Message type distribution",
            "SELECT message_type, COUNT(*) FROM transcripts GROUP BY message_type"
        )

        # Test 8: Content length analysis
        self.compare_query(
            "Average content length",
            "SELECT message_type, AVG(LENGTH(content)) as avg_len FROM transcripts GROUP BY message_type"
        )

        # Test 9: LIKE operator
        self.compare_query(
            "LIKE: Find 'fix' commands",
            "SELECT COUNT(*) FROM command_history WHERE display LIKE '%fix%'"
        )

        # Test 10: IN operator
        self.compare_query(
            "IN: Specific projects",
            "SELECT COUNT(*) FROM command_history WHERE project IN ('/Users/walker/code/HarnessDB', '/Users/walker/code/cicd')"
        )

        # Test 11: BETWEEN operator (timestamp range)
        self.compare_query(
            "BETWEEN: Timestamp range",
            "SELECT COUNT(*) FROM command_history WHERE timestamp BETWEEN 1774676000000 AND 1774677000000"
        )

        # Test 12: IS NULL check
        self.compare_query(
            "IS NULL: Empty pasted_text",
            "SELECT COUNT(*) FROM command_history WHERE pasted_text IS NULL OR pasted_text = ''"
        )

        # Test 13: Aggregate functions
        self.compare_query(
            "Aggregate: Project stats",
            """SELECT project,
                      COUNT(*) as total,
                      MIN(timestamp) as first_cmd,
                      MAX(timestamp) as last_cmd,
                      COUNT(DISTINCT session_id) as sessions
               FROM command_history
               GROUP BY project
               ORDER BY total DESC
               LIMIT 5"""
        )

        # Test 14: Subquery
        self.compare_query(
            "Subquery: Projects with > 50 commands",
            """SELECT project, cmd_count
               FROM (SELECT project, COUNT(*) as cmd_count FROM command_history GROUP BY project) t
               WHERE cmd_count > 50
               ORDER BY cmd_count DESC"""
        )

        # Test 15: JOIN simulation (union of both tables)
        self.compare_query(
            "Timestamp correlation",
            """SELECT 'command' as type, COUNT(*) as count
               FROM command_history
               WHERE timestamp > 1774676000000
               UNION ALL
               SELECT 'transcript' as type, COUNT(*) as count
               FROM transcripts
               WHERE timestamp_str > '2026-05-01'"""
        )

        print("\n" + "="*80)
        print("TEST SUITE COMPLETE")
        print("="*80)

def main():
    comparator = ClaudeLogsComparator()

    # Setup DuckDB
    comparator.setup_duckdb()

    # Run all tests
    comparator.run_all_tests()

if __name__ == '__main__':
    main()