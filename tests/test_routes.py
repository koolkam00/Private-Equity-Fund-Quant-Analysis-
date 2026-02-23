def test_index_redirect(client):
    """Test that the index page redirects to the dashboard."""
    response = client.get("/")
    assert response.status_code == 302
    assert "/dashboard" in response.location

def test_dashboard_page(client):
    """Test that the dashboard page loads successfully."""
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert b"Dashboard" in response.data

def test_upload_page(client):
    """Test that the upload page loads successfully."""
    response = client.get("/upload")
    assert response.status_code == 200
    assert b"Upload" in response.data

def test_deals_page(client):
    """Test that the deals page loads successfully."""
    response = client.get("/deals")
    assert response.status_code == 200
    assert b"Deals" in response.data

def test_cashflows_page(client):
    """Test that the cashflows page loads successfully."""
    response = client.get("/cashflows")
    assert response.status_code == 200
    assert b"Cashflows" in response.data
