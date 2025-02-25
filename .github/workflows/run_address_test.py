import pytest
import sys
from pathlib import Path


def run_test():
    """Run the address matcher test and output results for GitHub Actions"""
    # Run the test using pytest and capture the result
    pytest_args = [
        "-xvs",
        "tests/test_address_matcher.py::test_address_matching_combined",
    ]
    result = pytest.main(pytest_args)

    # If the test failed completely, exit with error
    if result != 0 and result != pytest.ExitCode.TESTS_FAILED:
        print("Error running tests")
        return 1

    # Get the test results from the pytest output capture
    test_results = getattr(pytest, "_test_results", None)

    if not test_results:
        print("No test results found")
        return 1

    # Format the GitHub comment
    match_rate = f"Match rate: {test_results['match_rate']:.2f}%"
    total = test_results["total_cases"]
    correct = test_results["correct_matches"]

    # Create GitHub Actions output
    with open(Path.cwd() / "github-comment.md", "w") as f:
        f.write("## 📊 Address Matcher Test Results\n\n")
        f.write(f"**{match_rate}** ({correct}/{total} addresses matched correctly)\n\n")

        if test_results["mismatches"]:
            f.write("### ⚠️ Some addresses were not matched correctly:\n\n")

            for mismatch in test_results["mismatches"]:
                f.write(f"#### Test Block ID: {mismatch['test_block_id']}\n\n")
                f.write("| Record Type | Address | Postcode | Match Weight |\n")
                f.write("| ----------- | ------- | -------- | ------------ |\n")

                for record in mismatch["records"]:
                    weight_str = (
                        f"{record['match_weight']:.2f}"
                        if record["match_weight"] is not None
                        else "N/A"
                    )
                    f.write(
                        f"| {record['record_type']} | {record['address']} | {record['postcode']} | {weight_str} |\n"
                    )

                f.write("\n")
        else:
            f.write("### ✅ All addresses were matched correctly!")

    print(f"Test completed: {match_rate}")
    return 0


if __name__ == "__main__":
    sys.exit(run_test())
