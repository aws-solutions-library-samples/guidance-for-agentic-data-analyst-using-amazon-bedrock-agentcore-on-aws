from pathlib import Path


REPOSITORY_DIR = Path(__file__).parent.parent.parent.parent
DATA_DIR = REPOSITORY_DIR / 'data'
QUERIES_PATH = DATA_DIR / 'queries.json'

TESTS_PATH = DATA_DIR / 'tests'
