from __future__ import annotations

from typing import Tuple


class DomainGuard:
    MESSAGE = "This system is designed to answer questions related to the provided dataset only."

    DOMAIN_TERMS = {
        "customer", "product", "sales", "sales order", "delivery", "billing",
        "billing document", "invoice", "journal", "journal entry", "plant",
        "material", "order", "quantity", "amount", "revenue", "flow",
        "purchase", "accounting", "erp",
    }

    BLOCK_TERMS = {
        "poem", "story", "joke", "weather", "movie", "politics", "recipe",
        "capital of", "who is", "write code for game", "fantasy", "romance",
    }

    @staticmethod
    def is_in_domain(question: str) -> Tuple[bool, str]:
        q = (question or "").strip().lower()
        if not q:
            return False, DomainGuard.MESSAGE

        if any(t in q for t in DomainGuard.BLOCK_TERMS):
            return False, DomainGuard.MESSAGE

        score = sum(1 for t in DomainGuard.DOMAIN_TERMS if t in q)
        if score == 0:
            return False, DomainGuard.MESSAGE

        return True, "ok"