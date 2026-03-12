from __future__ import annotations

import json
import logging
import os

from flask import current_app, has_app_context


logger = logging.getLogger(__name__)


def provider_enabled() -> bool:
    if not has_app_context():
        return False
    provider = (current_app.config.get("MEMO_LLM_PROVIDER") or "disabled").strip().lower()
    return provider == "openai" and bool(os.environ.get("OPENAI_API_KEY"))


def call_openai_json(model: str, prompt: dict, log: logging.Logger | None = None) -> dict | None:
    active_logger = log or logger
    if not provider_enabled():
        return None
    try:
        from openai import OpenAI
    except ImportError:
        active_logger.warning("OpenAI SDK not installed; falling back to deterministic memo generation.")
        return None

    client = OpenAI(timeout=float(current_app.config.get("MEMO_LLM_TIMEOUT_SECONDS", 90)))
    try:
        input_payload = json.dumps(prompt["input"], default=str)
        if hasattr(client, "responses"):
            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": [{"type": "input_text", "text": prompt["system"]}]},
                    {"role": "user", "content": [{"type": "input_text", "text": input_payload}]},
                ],
            )
            output_text = getattr(response, "output_text", None)
            if output_text:
                return json.loads(output_text)
        response = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt["system"]},
                {"role": "user", "content": input_payload},
            ],
        )
        output_text = response.choices[0].message.content
        return json.loads(output_text)
    except Exception:
        active_logger.exception("LLM memo generation failed; falling back to deterministic memo generation.")
        return None
