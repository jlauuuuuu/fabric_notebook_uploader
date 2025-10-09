#!/usr/bin/env python3
"""
Data Agent Testing Module

This module provides functionality for testing Fabric Data Agents locally
using the external client. It integrates with the fabric_notebook_uploader
project to provide comprehensive testing capabilities for data agents.

Features:
- Test data agents with predefined questions
- Extract and analyze SQL queries from lakehouse sources
- Generate test reports with data previews
- Save test results for analysis
"""

import os
import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Any
from .data_agent_client import FabricDataAgentClient, create_data_agent_client_from_env


class DataAgentTester:
    """
    A comprehensive testing framework for Fabric Data Agents.
    """
    
    def __init__(self, tenant_id: str = None, data_agent_url: str = None):
        """
        Initialize the Data Agent Tester.
        
        Args:
            tenant_id (str, optional): Azure tenant ID. If None, will try environment variables.
            data_agent_url (str, optional): Data agent URL. If None, will try environment variables.
        """
        if tenant_id and data_agent_url:
            self.client = FabricDataAgentClient(tenant_id, data_agent_url)
        else:
            self.client = create_data_agent_client_from_env()
            if not self.client:
                raise ValueError("Could not initialize data agent client. Please provide credentials.")
        
        self.test_results = []
        self.session_id = f"test_session_{int(time.time())}"
        print(f"🧪 Data Agent Tester initialized (Session: {self.session_id})")
    
    def run_simple_test(self, question: str, timeout: int = 120) -> Dict[str, Any]:
        """
        Run a simple test with a single question.
        
        Args:
            question (str): The question to ask the data agent
            timeout (int): Timeout in seconds
            
        Returns:
            dict: Test result containing question, response, and metadata
        """
        print(f"\n🔬 Running simple test...")
        start_time = time.time()
        
        try:
            response = self.client.ask(question, timeout)
            end_time = time.time()
            
            result = {
                "test_type": "simple",
                "question": question,
                "response": response,
                "success": True,
                "duration": end_time - start_time,
                "timestamp": datetime.now().isoformat(),
                "session_id": self.session_id
            }
            
            self.test_results.append(result)
            print(f"✅ Simple test completed in {result['duration']:.2f}s")
            return result
            
        except Exception as e:
            end_time = time.time()
            result = {
                "test_type": "simple",
                "question": question,
                "response": None,
                "success": False,
                "error": str(e),
                "duration": end_time - start_time,
                "timestamp": datetime.now().isoformat(),
                "session_id": self.session_id
            }
            
            self.test_results.append(result)
            print(f"❌ Simple test failed after {result['duration']:.2f}s: {e}")
            return result
    
    def run_detailed_test(self, question: str) -> Dict[str, Any]:
        """
        Run a detailed test that extracts SQL queries and data previews.
        
        Args:
            question (str): The question to ask the data agent
            
        Returns:
            dict: Detailed test result with SQL analysis
        """
        print(f"\n🔬 Running detailed test with SQL analysis...")
        start_time = time.time()
        
        try:
            run_details = self.client.get_run_details(question)
            end_time = time.time()
            
            # Analyze the results
            has_sql = bool(run_details.get("sql_queries"))
            has_data_preview = bool(run_details.get("sql_data_previews"))
            
            result = {
                "test_type": "detailed",
                "question": question,
                "run_details": run_details,
                "success": "error" not in run_details,
                "duration": end_time - start_time,
                "timestamp": datetime.now().isoformat(),
                "session_id": self.session_id,
                "analysis": {
                    "has_sql_queries": has_sql,
                    "sql_query_count": len(run_details.get("sql_queries", [])),
                    "has_data_preview": has_data_preview,
                    "lakehouse_detected": has_sql,
                    "run_status": run_details.get("run_status", "unknown")
                }
            }
            
            self.test_results.append(result)
            
            if result["success"]:
                print(f"✅ Detailed test completed in {result['duration']:.2f}s")
                print(f"📊 Analysis: SQL queries: {result['analysis']['sql_query_count']}, "
                      f"Data preview: {result['analysis']['has_data_preview']}")
            else:
                print(f"❌ Detailed test failed after {result['duration']:.2f}s")
            
            return result
            
        except Exception as e:
            end_time = time.time()
            result = {
                "test_type": "detailed",
                "question": question,
                "run_details": None,
                "success": False,
                "error": str(e),
                "duration": end_time - start_time,
                "timestamp": datetime.now().isoformat(),
                "session_id": self.session_id,
                "analysis": {
                    "has_sql_queries": False,
                    "sql_query_count": 0,
                    "has_data_preview": False,
                    "lakehouse_detected": False,
                    "run_status": "error"
                }
            }
            
            self.test_results.append(result)
            print(f"❌ Detailed test failed after {result['duration']:.2f}s: {e}")
            return result
    
    def run_test_suite(self, questions: List[str], include_detailed: bool = True) -> Dict[str, Any]:
        """
        Run a suite of tests with multiple questions.
        
        Args:
            questions (list): List of questions to test
            include_detailed (bool): Whether to run detailed tests
            
        Returns:
            dict: Suite results with summary statistics
        """
        print(f"\n🧪 Running test suite with {len(questions)} questions...")
        suite_start = time.time()
        
        suite_results = []
        
        for i, question in enumerate(questions, 1):
            print(f"\n📋 Test {i}/{len(questions)}: {question[:50]}...")
            
            if include_detailed:
                result = self.run_detailed_test(question)
            else:
                result = self.run_simple_test(question)
            
            suite_results.append(result)
            
            # Add delay between tests to avoid overwhelming the service
            if i < len(questions):
                print("⏳ Waiting 2 seconds before next test...")
                time.sleep(2)
        
        suite_end = time.time()
        
        # Calculate summary statistics
        successful_tests = sum(1 for result in suite_results if result["success"])
        failed_tests = len(suite_results) - successful_tests
        total_duration = suite_end - suite_start
        avg_duration = sum(result["duration"] for result in suite_results) / len(suite_results)
        
        sql_tests = sum(1 for result in suite_results 
                       if result.get("analysis", {}).get("has_sql_queries", False))
        
        summary = {
            "suite_type": "detailed" if include_detailed else "simple",
            "total_tests": len(questions),
            "successful_tests": successful_tests,
            "failed_tests": failed_tests,
            "success_rate": successful_tests / len(questions) * 100,
            "total_duration": total_duration,
            "average_duration": avg_duration,
            "sql_tests_detected": sql_tests,
            "timestamp": datetime.now().isoformat(),
            "session_id": self.session_id,
            "results": suite_results
        }
        
        print(f"\n📊 Test Suite Summary:")
        print(f"   Total Tests: {summary['total_tests']}")
        print(f"   Successful: {summary['successful_tests']}")
        print(f"   Failed: {summary['failed_tests']}")
        print(f"   Success Rate: {summary['success_rate']:.1f}%")
        print(f"   Total Duration: {summary['total_duration']:.2f}s")
        print(f"   Average Duration: {summary['average_duration']:.2f}s")
        if include_detailed:
            print(f"   SQL Tests Detected: {summary['sql_tests_detected']}")
        
        return summary
    
    def get_default_test_questions(self) -> List[str]:
        """
        Get a default set of test questions for data agents.
        
        Returns:
            list: Default test questions
        """
        return [
            "What data is available in the lakehouse?",
            "Show me the top 5 records from any available table",
            "What are the column names and types in the main tables?",
            "How many records are in the largest table?",
            "Show me a sample of the data with different data types",
            "What tables contain customer or user information?",
            "Give me a summary of the data quality",
            "What date ranges are covered in the time-series data?",
            "Show me the most recent data entries",
            "What are the unique values in categorical columns?"
        ]
    
    def run_comprehensive_test(self) -> Dict[str, Any]:
        """
        Run a comprehensive test using default questions.
        
        Returns:
            dict: Comprehensive test results
        """
        print("🚀 Starting comprehensive data agent test...")
        default_questions = self.get_default_test_questions()
        return self.run_test_suite(default_questions, include_detailed=True)
    
    def save_results(self, filename: str = None) -> str:
        """
        Save test results to a JSON file.
        
        Args:
            filename (str, optional): Output filename. If None, auto-generates based on session.
            
        Returns:
            str: Path to saved file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_agent_test_results_{timestamp}.json"
        
        # Create results directory if it doesn't exist
        results_dir = "test_results"
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
        
        filepath = os.path.join(results_dir, filename)
        
        # Prepare results for saving
        save_data = {
            "session_id": self.session_id,
            "timestamp": datetime.now().isoformat(),
            "total_tests": len(self.test_results),
            "results": self.test_results
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Test results saved to: {filepath}")
        return filepath
    
    def generate_report(self) -> str:
        """
        Generate a human-readable test report.
        
        Returns:
            str: Formatted test report
        """
        if not self.test_results:
            return "No test results available."
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("DATA AGENT TEST REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Session ID: {self.session_id}")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Total Tests: {len(self.test_results)}")
        report_lines.append("")
        
        # Summary statistics
        successful_tests = sum(1 for result in self.test_results if result["success"])
        failed_tests = len(self.test_results) - successful_tests
        
        report_lines.append("SUMMARY STATISTICS")
        report_lines.append("-" * 40)
        report_lines.append(f"Successful Tests: {successful_tests}")
        report_lines.append(f"Failed Tests: {failed_tests}")
        report_lines.append(f"Success Rate: {successful_tests / len(self.test_results) * 100:.1f}%")
        
        if any(result.get("analysis") for result in self.test_results):
            sql_tests = sum(1 for result in self.test_results 
                           if result.get("analysis", {}).get("has_sql_queries", False))
            report_lines.append(f"SQL Tests Detected: {sql_tests}")
        
        avg_duration = sum(result["duration"] for result in self.test_results) / len(self.test_results)
        report_lines.append(f"Average Duration: {avg_duration:.2f}s")
        report_lines.append("")
        
        # Individual test results
        report_lines.append("INDIVIDUAL TEST RESULTS")
        report_lines.append("-" * 40)
        
        for i, result in enumerate(self.test_results, 1):
            status = "✅ PASS" if result["success"] else "❌ FAIL"
            report_lines.append(f"\nTest {i}: {status}")
            report_lines.append(f"Question: {result['question']}")
            report_lines.append(f"Duration: {result['duration']:.2f}s")
            
            if not result["success"]:
                report_lines.append(f"Error: {result.get('error', 'Unknown error')}")
            
            if result.get("analysis"):
                analysis = result["analysis"]
                report_lines.append(f"SQL Queries: {analysis['sql_query_count']}")
                report_lines.append(f"Data Preview: {'Yes' if analysis['has_data_preview'] else 'No'}")
                report_lines.append(f"Lakehouse Detected: {'Yes' if analysis['lakehouse_detected'] else 'No'}")
        
        report_lines.append("\n" + "=" * 80)
        return "\n".join(report_lines)
    
    def print_report(self):
        """
        Print the test report to console.
        """
        print(self.generate_report())


def run_quick_test(question: str = "What data is available in the lakehouse?") -> Dict[str, Any]:
    """
    Run a quick test with a single question using environment configuration.
    
    Args:
        question (str): Question to ask the data agent
        
    Returns:
        dict: Test result
    """
    try:
        tester = DataAgentTester()
        return tester.run_simple_test(question)
    except Exception as e:
        print(f"❌ Quick test failed: {e}")
        return {"success": False, "error": str(e)}


def run_comprehensive_test() -> Dict[str, Any]:
    """
    Run a comprehensive test using default questions and environment configuration.
    
    Returns:
        dict: Comprehensive test results
    """
    try:
        tester = DataAgentTester()
        return tester.run_comprehensive_test()
    except Exception as e:
        print(f"❌ Comprehensive test failed: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    """
    Command-line interface for testing data agents.
    """
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "comprehensive":
            print("🚀 Running comprehensive data agent test...")
            results = run_comprehensive_test()
            if results.get("success", False):
                print("✅ Comprehensive test completed successfully!")
            else:
                print("❌ Comprehensive test failed!")
        elif sys.argv[1] == "quick":
            question = sys.argv[2] if len(sys.argv) > 2 else "What data is available in the lakehouse?"
            print(f"🚀 Running quick test with question: {question}")
            result = run_quick_test(question)
            if result.get("success", False):
                print("✅ Quick test completed successfully!")
                print(f"Response: {result.get('response', 'No response')}")
            else:
                print("❌ Quick test failed!")
        else:
            print("Usage: python -m fabric_notebook_uploader.test_agent [comprehensive|quick] [question]")
    else:
        print("🧪 Data Agent Testing Module")
        print("Usage:")
        print("  python -m fabric_notebook_uploader.test_agent comprehensive")
        print("  python -m fabric_notebook_uploader.test_agent quick [question]")
        print("\nOr use the DataAgentTester class programmatically.")