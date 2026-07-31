#!/usr/bin/env python3
"""
Comprehensive Protocol Comparison Test Suite
Tests all protocols (except Redis) against DuckDB for query consistency
"""

import subprocess
import json
import time
import tempfile
import os
from pathlib import Path

class ProtocolTester:
    def __init__(self, protocol_name, port, harness_binary='./target/release/harness-db'):
        self.protocol_name = protocol_name
        self.port = port
        self.harness_binary = harness_binary
        self.harness_process = None
        self.test_results = []

    def start_harness(self):
        """Start HarnessDB server for this protocol"""
        data_dir = tempfile.mkdtemp(prefix=f'harness_{self.protocol_name}_')
        meta_dir = tempfile.mkdtemp(prefix=f'harness_meta_{self.protocol_name}_')

        cmd = [
            self.harness_binary,
            '--dev',
            '--data-dir', data_dir,
            '--meta-dir', meta_dir,
            f'--{self.protocol_name}-port', str(self.port)
        ]

        self.harness_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(3)  # Wait for server to start
        print(f"✅ Started {self.protocol_name} server on port {self.port}")

    def stop_harness(self):
        """Stop HarnessDB server"""
        if self.harness_process:
            self.harness_process.terminate()
            self.harness_process.wait(timeout=5)
            print(f"🛑 Stopped {self.protocol_name} server")

    def run_test(self, test_name, harness_query, duckdb_query=None):
        """Run a test query on both databases and compare results"""
        if duckdb_query is None:
            duckdb_query = harness_query

        result = {
            'test': test_name,
            'protocol': self.protocol_name,
            'harness_result': None,
            'duckdb_result': None,
            'match': False,
            'error': None
        }

        try:
            # Run HarnessDB query
            harness_result = self.execute_harness_query(harness_query)
            result['harness_result'] = harness_result

            # Run DuckDB query
            duckdb_result = self.execute_duckdb_query(duckdb_query)
            result['duckdb_result'] = duckdb_result

            # Compare results
            result['match'] = self.compare_results(harness_result, duckdb_result)

        except Exception as e:
            result['error'] = str(e)

        self.test_results.append(result)
        return result

    def execute_harness_query(self, query):
        """Execute query on HarnessDB - protocol-specific"""
        raise NotImplementedError("Subclass must implement")

    def execute_duckdb_query(self, query):
        """Execute query on DuckDB"""
        # Use duckdb CLI or Python API
        import duckdb
        conn = duckdb.connect(':memory:')
        result = conn.execute(query).fetchall()
        conn.close()
        return result

    def compare_results(self, harness_result, duckdb_result):
        """Compare results from both databases"""
        return str(harness_result) == str(duckdb_result)

    def generate_report(self):
        """Generate test report"""
        passed = sum(1 for r in self.test_results if r['match'])
        failed = len(self.test_results) - passed

        report = f"\n{'='*60}\n"
        report += f"{self.protocol_name.upper()} Protocol Test Results\n"
        report += f"{'='*60}\n"
        report += f"Total Tests: {len(self.test_results)}\n"
        report += f"Passed: {passed} ✅\n"
        report += f"Failed: {failed} ❌\n"
        report += f"Success Rate: {passed/len(self.test_results)*100:.1f}%\n"
        report += f"{'='*60}\n\n"

        for result in self.test_results:
            status = "✅ PASS" if result['match'] else "❌ FAIL"
            report += f"{status} - {result['test']}\n"
            if not result['match'] and result['error']:
                report += f"  Error: {result['error']}\n"

        return report


class MySQLProtocolTester(ProtocolTester):
    def __init__(self, port=3307):
        super().__init__('mysql', port)

    def execute_harness_query(self, query):
        """Execute MySQL query on HarnessDB"""
        cmd = [
            'mysql', '-h', '127.0.0.1', '-P', str(self.port),
            '-uroot', '-e', query, '-t'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout


class PostgreSQLProtocolTester(ProtocolTester):
    def __init__(self, port=5433):
        super().__init__('pg', port)

    def execute_harness_query(self, query):
        """Execute PostgreSQL query on HarnessDB"""
        # Use psql client
        cmd = ['psql', '-h', '127.0.0.1', '-p', str(self.port),
               '-U', 'harness', '-c', query]
        env = {'PGPASSWORD': 'test123'}
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        return result.stdout


class ClickHouseProtocolTester(ProtocolTester):
    def __init__(self, port=8124):
        super().__init__('clickhouse', port)

    def execute_harness_query(self, query):
        """Execute ClickHouse HTTP query"""
        import requests
        url = f'http://127.0.0.1:{self.port}/?query={query}'
        response = requests.get(url)
        return response.text


def main():
    print("="*80)
    print("COMPREHENSIVE PROTOCOL TESTING: HarnessDB vs DuckDB")
    print("="*80)

    testers = [
        MySQLProtocolTester(port=3307),
        PostgreSQLProtocolTester(port=5433),
        ClickHouseProtocolTester(port=8124),
    ]

    all_reports = []

    for tester in testers:
        print(f"\n{'='*80}")
        print(f"Testing {tester.protocol_name.upper()} Protocol")
        print(f"{'='*80}")

        try:
            tester.start_harness()

            # Basic data type tests
            tester.run_test(
                "Integer operations",
                "SELECT 1 + 2 AS result",
                "SELECT 1 + 2 AS result"
            )

            tester.run_test(
                "String operations",
                "SELECT 'hello' || ' world' AS result",
                "SELECT 'hello' || ' world' AS result"
            )

            # Add more tests...

        finally:
            tester.stop_harness()

        report = tester.generate_report()
        all_reports.append(report)
        print(report)

    # Write combined report
    with open('/tmp/protocol_comparison_report.txt', 'w') as f:
        f.write('\n'.join(all_reports))

    print("\n" + "="*80)
    print("ALL PROTOCOL TESTS COMPLETED")
    print("="*80)


if __name__ == '__main__':
    main()