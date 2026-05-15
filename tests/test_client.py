import responses

from pik.client import PikClient, PikApiError


BLOCK_URL = "https://api.pik.ru/v2/flat"


def _page(page: int, count: int, n: int):
    return {
        "block": {"id": 1165, "name": "Нарвин"},
        "count": count,
        "flats": [
            {
                "id": page * 100 + i,
                "guid": f"g-{page}-{i}",
                "block_id": 1165,
                "bulk_id": 10395,
                "rooms": 1,
                "floor": 1,
                "area": 30.0,
                "price": 1000,
                "meterPrice": 100,
                "status": "free",
                "name": "x",
                "url": "x",
                "updatedAt": "x",
            }
            for i in range(n)
        ],
    }


@responses.activate
def test_fetch_paginates_until_empty():
    responses.add(responses.GET, BLOCK_URL, json=_page(1, 75, 50), status=200)
    responses.add(responses.GET, BLOCK_URL, json=_page(2, 75, 25), status=200)
    responses.add(responses.GET, BLOCK_URL, json=_page(3, 75, 0), status=200)

    client = PikClient(retries=0, backoff=lambda _i: 0)
    items = client.fetch_block_flats(block_id=1165, types=(1,))

    assert len(items) == 75
    assert items[0]["id"] == 100
    assert items[-1]["id"] == 224


@responses.activate
def test_fetch_retries_on_502():
    responses.add(responses.GET, BLOCK_URL, status=502)
    responses.add(responses.GET, BLOCK_URL, status=502)
    responses.add(responses.GET, BLOCK_URL, json=_page(1, 1, 1), status=200)
    responses.add(responses.GET, BLOCK_URL, json=_page(2, 1, 0), status=200)

    client = PikClient(retries=2, backoff=lambda _i: 0)
    items = client.fetch_block_flats(block_id=1165, types=(1,))

    assert len(items) == 1


@responses.activate
def test_fetch_gives_up_after_retries():
    for _ in range(4):
        responses.add(responses.GET, BLOCK_URL, status=502)

    client = PikClient(retries=2, backoff=lambda _i: 0)
    try:
        client.fetch_block_flats(block_id=1165, types=(1,))
    except PikApiError:
        return
    raise AssertionError("PikApiError not raised")


@responses.activate
def test_fetch_sends_browser_user_agent():
    responses.add(responses.GET, BLOCK_URL, json=_page(1, 0, 0), status=200)
    client = PikClient(retries=0, backoff=lambda _i: 0)
    client.fetch_block_flats(block_id=1165, types=(1,))
    ua = responses.calls[0].request.headers["User-Agent"]
    assert "Mozilla" in ua and "AppleWebKit" in ua
