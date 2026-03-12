from __future__ import annotations

from peqa.services.memos.jobs import run_worker_loop


def run_memo_worker(poll_interval: float = 2.0):
    run_worker_loop(poll_interval=poll_interval)
