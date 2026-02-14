# -*- coding: utf-8 -*-

"""FastAPI wrapper for app.py review logic.

Constraint: do NOT change business logic. Only expose JSON endpoints.
"""

import datetime
from typing import Optional

from fastapi import FastAPI, Query

from backend import review_logic

app = FastAPI(title="ch-stock api", version="0.1")


def _parse_date(date_str: str) -> datetime.date:
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/review")
def get_review(
    date: str = Query(..., description="YYYY-MM-DD"),
    external: bool = True,
    market: bool = True,
    top100: bool = True,
    short: bool = True,
    use_cache: bool = True,
    write_cache: bool = True,
):
    """Return review_data (same structure as app.py build_review_data output)."""

    d = _parse_date(date)
    show_modules = {"external": external, "market": market, "top100": top100, "short": short}

    if use_cache:
        cached = review_logic.load_review_data(date)
        if cached:
            return {"source": "cache", "data": cached}

    data = review_logic.build_review_data(d, show_modules)

    if write_cache and review_logic.is_review_data_complete(data):
        review_logic.save_review_data(date, data)
        return {"source": "fresh_saved", "data": data}

    return {"source": "fresh", "data": data}
