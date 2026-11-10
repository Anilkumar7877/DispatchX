import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

# Ensure dispatchx package root is on sys.path
package_root = Path(__file__).parent.parent
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

from app.main import app

@pytest.fixture(scope="session")
def client():
    with TestClient(app) as test_client:
        yield test_client
