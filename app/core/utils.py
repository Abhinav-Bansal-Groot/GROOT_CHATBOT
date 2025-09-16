from bson import ObjectId
from datetime import datetime
import json


def serialize_doc_for_payload(doc: dict) -> dict:
    new = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            new[k] = str(v)
        elif isinstance(v, datetime):
            new[k] = v.isoformat()
        else:
            try:
                json.dumps({k: v})
                new[k] = v
            except (TypeError, OverflowError):
                new[k] = str(v)
    return new


def doc_to_text(doc: dict, preferred_fields: list[str] | None = None) -> str:
    """Convert a Mongo document to a single text string for embedding/search.
    If preferred_fields provided, use them in order. some changes
    """
    if preferred_fields:
        parts = [str(doc.get(f)) for f in preferred_fields if f in doc and doc.get(f)]
        if any(parts):
            return " ".join(parts)


    # fallback: join all string-like fields
    parts = [str(v) for v in doc.values() if isinstance(v, str) and v.strip()]
    return " ".join(parts)