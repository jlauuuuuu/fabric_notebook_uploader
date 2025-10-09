#!/usr/bin/env python3
"""
Simple data agent testing script using the configuration system.

Usage:
    python test_agent.py [data_agent_name]
    
Tests deployed data agents locally with basic questions.
"""

import sys
from fabric_notebook_uploader import run_quick_test
from config import config

def main():
    """Test a deployed data agent."""
    # Get data agent name from command line
    if len(sys.argv) < 2:
        print("Usage: python test_agent.py [data_agent_name]")
        
        # Show available agents with test URLs
        workspace = config.get_active_workspace()
        if workspace:
            agents = workspace.list_data_agents()
            testable = [name for name in agents if workspace.get_data_agent(name).test_url]
            if testable:
                print(f"Available agents: {', '.join(testable)}")
        return
    
    agent_name = sys.argv[1]
    
    # Check if agent exists and has test URL
    workspace = config.get_active_workspace()
    if not workspace:
        print("❌ No active workspace configured")
        return
    
    agent = workspace.get_data_agent(agent_name)
    if not agent or not agent.test_url:
        print(f"❌ Agent '{agent_name}' not found or no test URL configured")
        return
    
    print(f"🧪 Testing Data Agent: {agent_name}")
    print(f"🔗 Test URL: {agent.test_url}")
    print()
    
    # Run basic tests
    test_questions = [
        "What data is available?",
        "Show me a summary of the data",
        "What tables are available?"
    ]
    
    for question in test_questions:
        print(f"❓ Question: {question}")
        result = run_quick_test(question)
        
        if result["success"]:
            print(f"✅ Response: {result['response'][:100]}...")
        else:
            print(f"❌ Error: {result['error']}")
        print()
    
    # Update agent status
    config.update_data_agent(agent_name, status="tested")
    print(f"✅ Agent '{agent_name}' tested successfully!")

if __name__ == "__main__":
    main()