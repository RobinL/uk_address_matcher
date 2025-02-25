import sys
from io import StringIO
import pytest

# Redirect stdout to capture output
old_stdout = sys.stdout
sys.stdout = mystdout = StringIO()

# Run the specific test
pytest.main(["tests/test_address_matcher.py::test_address_matching_combined", "-v"])

# Get the captured output
output = mystdout.getvalue()
sys.stdout = old_stdout

# Save output to file for the next step
with open("test_results.txt", "w") as f:
    f.write(output)

# Print output for logs
print(output)
