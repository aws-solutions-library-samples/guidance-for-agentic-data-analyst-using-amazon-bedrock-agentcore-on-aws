from pathlib import Path


REPOSITORY_DIR = Path(__file__).parent.parent.parent.parent
TEST_DATA_DIR = REPOSITORY_DIR / 'test'
TESTS_PATH = TEST_DATA_DIR / 'queries.json'
