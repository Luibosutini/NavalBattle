#!/usr/bin/env python3
"""
Integration Test - End-to-end testing for Naval Battle system
Tests: CVL→DD→CL→CA flow, NEED_INPUT/APPROVAL, budget gates
"""
import os
import sys
import time
import json
import boto3
from datetime import datetime

# Set Bedrock model IDs before importing other modules
os.environ["BEDROCK_SONNET_MODEL_ID"] = "arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/jp.anthropic.claude-sonnet-4-5-20250929-v1:0"
os.environ["BEDROCK_OPUS_MODEL_ID"] = "arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/global.anthropic.claude-opus-4-6-v1"
os.environ["BEDROCK_MICRO_MODEL_ID"] = "arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/apac.amazon.nova-micro-v1:0"
os.environ["BEDROCK_LITE_MODEL_ID"] = "arn:aws:bedrock:ap-northeast-1:249033470572:inference-profile/apac.amazon.nova-lite-v1:0"

REGION = os.getenv("FLEET_REGION", "ap-northeast-1")
MISSION_STATE_TABLE = os.getenv("FLEET_STATE_TABLE", "fleet-mission-state")
TASK_STATE_TABLE = os.getenv("FLEET_TASK_STATE_TABLE", "fleet-task-state")
BUDGET_TABLE = os.getenv("FLEET_BUDGET_TABLE", "fleet-budget")
BUCKET = os.getenv("FLEET_S3_BUCKET", "")
SQS_NAME = os.getenv("FLEET_SQS_NAME", "fleet-missions")

dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)

TEST_TASK_ID = f"TEST-{int(time.time())}"
TEST_MONTH = "2099-12"  # Use future month to avoid conflicts


class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.message = ""
        self.start_time = time.time()
        self.end_time = None
    
    def success(self, message: str = ""):
        self.passed = True
        self.message = message
        self.end_time = time.time()
    
    def fail(self, message: str):
        self.passed = False
        self.message = message
        self.end_time = time.time()
    
    def duration(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time
    
    def __str__(self) -> str:
        status = "✓ PASS" if self.passed else "✗ FAIL"
        duration = f"({self.duration():.2f}s)"
        msg = f": {self.message}" if self.message else ""
        return f"{status} {self.name} {duration}{msg}"


def wait_for_mission_status(mission_id: str, expected_status: str, timeout: int = 60) -> bool:
    """Wait for mission to reach expected status"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = dynamodb.get_item(
                TableName=MISSION_STATE_TABLE,
                Key={"mission_id": {"S": mission_id}}
            )
            item = resp.get("Item")
            if item:
                status = item.get("status", {}).get("S", "")
                if status == expected_status:
                    return True
        except Exception as e:
            print(f"Error checking mission status: {e}")
        time.sleep(2)
    return False


def wait_for_task_status(task_id: str, expected_status: str, timeout: int = 60) -> bool:
    """Wait for task to reach expected status"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = dynamodb.get_item(
                TableName=TASK_STATE_TABLE,
                Key={"task_id": {"S": task_id}}
            )
            item = resp.get("Item")
            if item:
                status = item.get("status", {}).get("S", "")
                if status == expected_status:
                    return True
        except Exception as e:
            print(f"Error checking task status: {e}")
        time.sleep(2)
    return False


def cleanup_test_data(task_id: str) -> None:
    """Clean up test data"""
    try:
        # Delete task
        dynamodb.delete_item(
            TableName=TASK_STATE_TABLE,
            Key={"task_id": {"S": task_id}}
        )
        print(f"Cleaned up task {task_id}")
    except Exception as e:
        print(f"Error cleaning up task: {e}")


def test_task_initialization() -> TestResult:
    """Test 1: Task initialization"""
    result = TestResult("Task Initialization")
    
    try:
        # Create test ticket
        ticket_content = f"# Test Ticket {TEST_TASK_ID}\n\nThis is a test ticket for integration testing."
        ticket_key = f"tickets/{TEST_TASK_ID}.md"
        s3.put_object(
            Bucket=BUCKET,
            Key=ticket_key,
            Body=ticket_content.encode("utf-8"),
            ContentType="text/markdown"
        )
        ticket_s3 = f"s3://{BUCKET}/{ticket_key}"
        
        # Initialize task (CVL -> CA)
        from task_orchestrator import init_task
        init_task(TEST_TASK_ID, ticket_s3, TEST_MONTH, ["CVL", "CA"])
        
        # Verify task exists
        resp = dynamodb.get_item(
            TableName=TASK_STATE_TABLE,
            Key={"task_id": {"S": TEST_TASK_ID}}
        )
        
        if "Item" in resp:
            item = resp["Item"]
            status = item.get("status", {}).get("S", "")
            if status == "ENQUEUED":
                result.success(f"Task {TEST_TASK_ID} initialized")
            else:
                result.fail(f"Task status is {status}, expected ENQUEUED")
        else:
            result.fail("Task not found in DynamoDB")
    
    except Exception as e:
        result.fail(str(e))
    
    return result


def test_cvl_mission() -> TestResult:
    """Test 2: CVL mission execution"""
    result = TestResult("CVL Mission Execution")
    
    try:
        from task_orchestrator import start_task, get_task_state
        
        # Start task (enqueues CVL)
        start_task(TEST_TASK_ID)
        
        # Get mission ID
        task = get_task_state(TEST_TASK_ID)
        if not task:
            result.fail("Task not found")
            return result
        
        stages = task.get("stages", {}).get("M", {})
        cvl_info = stages.get("CVL", {}).get("M", {})
        mission_id = cvl_info.get("mission_id", {}).get("S")
        
        if not mission_id:
            result.fail("CVL mission not enqueued")
            return result
        
        # Wait for CVL to complete (or timeout)
        if wait_for_mission_status(mission_id, "DONE", timeout=120):
            result.success(f"CVL mission {mission_id} completed")
        else:
            result.fail(f"CVL mission {mission_id} did not complete in time")
    
    except Exception as e:
        result.fail(str(e))
    
    return result


def test_stage_progression() -> TestResult:
    """Test 3: Stage progression (CVL→CA)"""
    result = TestResult("Stage Progression")
    
    try:
        from task_orchestrator import advance_task, get_task_state
        
        # Advance to CA
        advance_task(TEST_TASK_ID)
        time.sleep(2)
        
        # Check current stage
        task = get_task_state(TEST_TASK_ID)
        if not task:
            result.fail("Task not found when checking CA")
            return result
        
        current_stage = task.get("current_stage", {}).get("S", "")
        if current_stage != "CA":
            result.fail(f"Expected stage CA, got {current_stage}")
            return result
        
        # Get mission ID
        stages_map = task.get("stages", {}).get("M", {})
        stage_info = stages_map.get("CA", {}).get("M", {})
        mission_id = stage_info.get("mission_id", {}).get("S")
        
        if not mission_id:
            result.fail("CA mission not enqueued")
            return result
        
        # Wait for completion
        if not wait_for_mission_status(mission_id, "DONE", timeout=120):
            result.fail("CA mission did not complete")
            return result
        
        result.success("Stage progression successful")
    
    except Exception as e:
        result.fail(str(e))
    
    return result


def test_budget_gate() -> TestResult:
    """Test 4: Budget gate (deny mission when budget exhausted)"""
    result = TestResult("Budget Gate")
    
    try:
        # Create a test mission with exhausted budget
        test_mission_id = f"M-BUDGET-TEST-{int(time.time())}"
        
        # Set budget to 0
        dynamodb.update_item(
            TableName=BUDGET_TABLE,
            Key={"month": {"S": TEST_MONTH}},
            UpdateExpression="SET fuel_remaining_usd = :zero",
            ExpressionAttributeValues={":zero": {"N": "0"}}
        )
        
        # Try to enqueue a mission
        queue_url = sqs.get_queue_url(QueueName=SQS_NAME)["QueueUrl"]
        payload = {
            "mission_id": test_mission_id,
            "task_id": "BUDGET-TEST",
            "ship_id": "DD-01",
            "ship_class": "DD",
            "budget": {"month": TEST_MONTH}
        }
        
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload)
        )
        
        # Wait for BUDGET_DENIED status
        if wait_for_mission_status(test_mission_id, "BUDGET_DENIED", timeout=30):
            result.success("Budget gate correctly denied mission")
        else:
            result.fail("Mission was not denied by budget gate")
        
        # Restore budget
        dynamodb.update_item(
            TableName=BUDGET_TABLE,
            Key={"month": {"S": TEST_MONTH}},
            UpdateExpression="SET fuel_remaining_usd = :cap",
            ExpressionAttributeValues={":cap": {"N": "30.0"}}
        )
    
    except Exception as e:
        result.fail(str(e))
    
    return result


def test_ca_escalation_logic() -> TestResult:
    """Test 5: CA escalation logic (BB skip)"""
    result = TestResult("CA Escalation Logic")
    
    try:
        from task_orchestrator import check_ca_escalation
        
        # Create mock CA report without BB keywords
        test_mission_id = f"M-CA-TEST-{int(time.time())}"
        report_content = """# CA Report
        
## Situation Summary
Task completed successfully.

## Findings
- All tests passed
- No issues found

## Next Orders
Task is complete. No further action needed.
"""
        
        report_key = f"missions/{test_mission_id}/artifacts/report.CA-01.md"
        s3.put_object(
            Bucket=BUCKET,
            Key=report_key,
            Body=report_content.encode("utf-8"),
            ContentType="text/markdown"
        )
        
        # Check escalation
        should_escalate = check_ca_escalation(test_mission_id)
        
        if not should_escalate:
            result.success("CA correctly decided no BB needed")
        else:
            result.fail("CA incorrectly requested BB escalation")
    
    except Exception as e:
        result.fail(str(e))
    
    return result


def run_all_tests() -> None:
    """Run all integration tests"""
    print("="*60)
    print("Naval Battle - Integration Test Suite")
    print("="*60)
    print(f"Test Task ID: {TEST_TASK_ID}")
    print(f"Test Month: {TEST_MONTH}")
    print()
    
    # Ensure test budget exists
    try:
        from task_orchestrator import now
        dynamodb.put_item(
            TableName=BUDGET_TABLE,
            Item={
                "month": {"S": TEST_MONTH},
                "fuel_cap_usd": {"N": "30.0"},
                "fuel_remaining_usd": {"N": "30.0"},
                "fuel_burned_usd": {"N": "0"},
                "sorties_total": {"N": "0"},
                "ammo_caps": {"M": {
                    "BB_main_gun": {"N": "4"},
                    "CA_salvo": {"N": "40"},
                    "CVB_air_wing": {"N": "20"}
                }},
                "ammo_used": {"M": {
                    "BB_main_gun": {"N": "0"},
                    "CA_salvo": {"N": "0"},
                    "CVB_air_wing": {"N": "0"}
                }},
                "updated_at": {"N": str(int(time.time()))}
            },
            ConditionExpression="attribute_not_exists(#m)",
            ExpressionAttributeNames={"#m": "month"}
        )
        print("✓ Test budget initialized\n")
    except Exception as e:
        if "ConditionalCheckFailedException" not in str(e):
            print(f"Warning: Could not initialize test budget: {e}\n")
    
    # Run tests
    tests = [
        test_task_initialization,
        test_cvl_mission,
        test_stage_progression,
        test_budget_gate,
        test_ca_escalation_logic,
    ]
    
    results = []
    for test_func in tests:
        print(f"Running: {test_func.__doc__}...")
        result = test_func()
        results.append(result)
        print(result)
        print()
        
        # Stop on first failure for sequential tests
        if not result.passed and test_func in [test_task_initialization, test_cvl_mission, test_stage_progression]:
            print("Stopping sequential tests due to failure")
            break
    
    # Summary
    print("="*60)
    print("Test Summary")
    print("="*60)
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    print(f"Failed: {total - passed}/{total}")
    print()
    
    for result in results:
        print(result)
    
    # Cleanup
    print()
    print("Cleaning up test data...")
    cleanup_test_data(TEST_TASK_ID)
    
    # Exit code
    sys.exit(0 if passed == total else 1)


def main():
    if not BUCKET:
        print("Error: FLEET_S3_BUCKET not set")
        sys.exit(1)
    
    run_all_tests()


if __name__ == "__main__":
    main()
