"""Тонкий клиент к публичному PIK JSON API."""
from __future__ import annotations

import time
from typing import Callable, Iterable

import requests


DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
BASE_URL = "https://api.pik.ru/v2/flat"

_RETRYABLE_STATUS = (429, 502, 503, 504)


class PikApiError(RuntimeError):
    pass


def _default_backoff(attempt: int) -> float:
    return [1, 5, 15, 45][min(attempt, 3)]


class PikClient:
    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_UA,
        retries: int = 3,
        backoff: Callable[[int], float] = _default_backoff,
        timeout: float = 20.0,
        session: requests.Session | None = None,
    ):
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": "https://www.pik.ru/",
        })

    def fetch_block_flats(
        self, *, block_id: int, types: Iterable[int] = (1,)
    ) -> list[dict]:
        items: list[dict] = []
        page = 1
        while True:
            payload = self._get_page(block_id=block_id, types=types, page=page)
            flats = payload.get("flats") or []
            if not flats:
                break
            items.extend(flats)
            page += 1
            if page > 200:
                raise PikApiError(f"pagination runaway at page {page}")
        return items

    def _get_page(self, *, block_id: int, types: Iterable[int], page: int) -> dict:
        params = {
            "block_id": block_id,
            "types": ",".join(str(t) for t in types),
            "page": page,
        }
        last_exc: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(BASE_URL, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_exc = exc
            else:
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except ValueError as exc:
                        last_exc = PikApiError(f"non-JSON body: {exc}")
                elif resp.status_code in _RETRYABLE_STATUS:
                    last_exc = PikApiError(f"HTTP {resp.status_code}")
                else:
                    raise PikApiError(
                        f"HTTP {resp.status_code} for page={page}: "
                        f"{resp.text[:200]}"
                    )
            if attempt < self.retries:
                time.sleep(self.backoff(attempt))
        raise PikApiError(
            f"page {page} failed after {self.retries + 1} attempts: {last_exc}"
        )
