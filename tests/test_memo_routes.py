def test_memo_pages_render(client):
    response = client.get("/memos")
    assert response.status_code == 200
    assert b"AI Memo Studio" in response.data
    assert b"Learn Your Style" in response.data
    assert b"Generate and Review" in response.data

    response = client.get("/memos/style-library")
    assert response.status_code == 200
    assert b"Style Library" in response.data
    assert b"Build a stronger style corpus for the memo engine" in response.data

    response = client.get("/memos/source-library")
    assert response.status_code == 200
    assert b"Source Library" in response.data
    assert b"Curate the diligence pack that grounds each memo run" in response.data
