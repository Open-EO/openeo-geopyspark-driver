from pathlib import Path

TEST_DATA_ROOT = Path(__file__).parent


def get_test_data_file(path: str) -> Path:
    """Get path of test data by relative path."""
    return TEST_DATA_ROOT / path
