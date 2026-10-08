from fastapi.testclient import TestClient

from api.main import app

# We bypass the lifespan DB pool for basic static UI testing
client = TestClient(app)


def test_ui_mount_exists():
    """Testa se o ponto de montagem /ui responde com a interface estática"""
    response = client.get("/ui")
    # Should redirect to /ui/ or return 200 directly
    assert response.status_code in [200, 307, 308]


def test_alerts_endpoint_without_db():
    """Testa se a API responde 500 caso o banco não esteja mockado/inicializado adequadamente"""
    response = client.get("/api/v1/alerts")
    assert response.status_code == 500
    assert response.json() == {"detail": "Database connection pool is not initialized"}
