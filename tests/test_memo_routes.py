def test_memo_pages_render(client):
    response = client.get("/memos")
    assert response.status_code == 200
    assert b"AI Investment Memos" in response.data

    response = client.get("/memos/style-library")
    assert response.status_code == 200
    assert b"Style Library" in response.data

    response = client.get("/memos/source-library")
    assert response.status_code == 200
    assert b"Source Library" in response.data
