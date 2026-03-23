from __future__ import annotations

from typing import Tuple


class DomainGuard:
    ERP_KEYWORDS: set[str] = {
        "customer",
        "order",
        "delivery",
        "billing",
        "sales",
        "product",
        "journal",
        "invoice",
        "shipment",
        "quantity",
        "amount",
        "document",
        "erp",
        "supply chain",
        "procurement",
        "material",
        "warehouse",
        "inventory",
    }

    OUT_OF_DOMAIN_PHRASES: set[str] = {
        "president",
        "country",
        "weather",
        "sports",
        "politics",
        "movie",
        "poem",
        "recipe",
        "joke",
        "write a",
    }

    @staticmethod
    def is_in_domain(question: str) -> Tuple[bool, str]:
        """Check if question is about ERP data."""
        q_lower = question.lower()

        # Check for out-of-domain phrases
        for phrase in DomainGuard.OUT_OF_DOMAIN_PHRASES:
            if phrase in q_lower:
                return False, "This system only answers questions about the ERP dataset."

        # Check for ERP keywords
        erp_match_count = sum(1 for kw in DomainGuard.ERP_KEYWORDS if kw in q_lower)

        if erp_match_count == 0:
            return False, "This system only answers questions about the ERP dataset."

        return True, ""