import subprocess
import os


def test_full_example():
    env = os.environ.copy()
    env["EPC_PATH"] = (
        f"read_csv('{os.path.abspath('tests/test_data/epc_fake.csv')}', filename=true)"
    )
    env["FULL_OS_PATH"] = (
        f"read_csv('{os.path.abspath('tests/test_data/os_fake.csv')}', filename=true)"
    )

    result = subprocess.run(
        ["python", "examples/match_epc_to_os.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
