"""Authoritative model pricing for read-only reviews (WS-R).

A review's cost ceiling can only be enforced if the review knows the **real** price of the
model it runs. Trusting a provider-reported ``cost_usd`` is not enough: many providers report
``0`` (or omit cost), which would silently disable the ceiling. Instead every review model must
have an *authoritative* price — either from the built-in :data:`KNOWN_MODEL_PRICES` map or from
an explicit ``KEEL_REVIEW_MODEL_PRICES`` operator override (input/output USD per 1M tokens).

An allowed-but-unpriced model is a configuration error that **fails closed** (``ReviewValidation
Error``) rather than running a cloud model with an unenforceable budget.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import ReviewValidationError

_TOKENS_PER_UNIT = 1_000_000


@dataclass(frozen=True, slots=True)
class ModelPrice:
    """Input/output price for a model, in USD per 1,000,000 tokens."""

    input_per_million: float
    output_per_million: float

    def cost(self, prompt_tokens: int, completion_tokens: int) -> float:
        prompt = max(0, prompt_tokens)
        completion = max(0, completion_tokens)
        return (
            prompt * self.input_per_million + completion * self.output_per_million
        ) / _TOKENS_PER_UNIT


# Built-in price map (USD per 1M tokens). Deliberately conservative and override-able: an
# operator sets ``KEEL_REVIEW_MODEL_PRICES`` to correct or extend these for their contract. Keys
# are normalized (lower-cased, provider prefix stripped) before lookup.
KNOWN_MODEL_PRICES: dict[str, ModelPrice] = {
    "gpt-4o": ModelPrice(2.5, 10.0),
    "gpt-4o-mini": ModelPrice(0.15, 0.6),
    "gpt-4.1": ModelPrice(2.0, 8.0),
    "gpt-4.1-mini": ModelPrice(0.4, 1.6),
    "gpt-4.1-nano": ModelPrice(0.1, 0.4),
    "o3": ModelPrice(2.0, 8.0),
    "o3-mini": ModelPrice(1.1, 4.4),
    "o4-mini": ModelPrice(1.1, 4.4),
    "claude-3-5-sonnet": ModelPrice(3.0, 15.0),
    "claude-3-5-haiku": ModelPrice(0.8, 4.0),
    "claude-3-7-sonnet": ModelPrice(3.0, 15.0),
    "claude-3-opus": ModelPrice(15.0, 75.0),
}


def _normalize(model: str) -> str:
    name = model.strip().lower()
    # Drop a single leading ``provider/`` routing prefix (e.g. ``openai/gpt-4o``).
    if "/" in name:
        name = name.split("/", 1)[1]
    return name


def _parse_override_entry(entry: str) -> tuple[str, ModelPrice] | None:
    """Parse one ``model=input/output`` override entry (rates USD per 1M tokens)."""
    entry = entry.strip()
    if not entry or "=" not in entry:
        return None
    model, _, rates = entry.partition("=")
    model = model.strip()
    if not model or "/" not in rates:
        return None
    in_raw, _, out_raw = rates.strip().partition("/")
    try:
        input_rate = float(in_raw.strip())
        output_rate = float(out_raw.strip())
    except ValueError:
        return None
    if input_rate < 0 or output_rate < 0:
        return None
    return model, ModelPrice(input_rate, output_rate)


def parse_price_overrides(raw: str) -> dict[str, ModelPrice]:
    """Parse ``KEEL_REVIEW_MODEL_PRICES`` (``model=in/out,model2=in/out``) into a price map."""
    overrides: dict[str, ModelPrice] = {}
    for entry in raw.split(","):
        parsed = _parse_override_entry(entry)
        if parsed is not None:
            model, price = parsed
            overrides[_normalize(model)] = price
    return overrides


@dataclass(frozen=True, slots=True)
class PriceBook:
    """Resolves a model to an authoritative price (operator override first, then known map)."""

    overrides: dict[str, ModelPrice]

    @classmethod
    def from_settings(cls, raw_overrides: str) -> PriceBook:
        return cls(overrides=parse_price_overrides(raw_overrides))

    def price_for(self, model: str) -> ModelPrice | None:
        key = _normalize(model)
        if key in self.overrides:
            return self.overrides[key]
        return KNOWN_MODEL_PRICES.get(key)

    def is_priced(self, model: str) -> bool:
        return self.price_for(model) is not None

    def cost_for(self, model: str, prompt_tokens: int, completion_tokens: int) -> float:
        price = self.price_for(model)
        if price is None:
            raise ReviewValidationError(
                f"review model {model!r} has no authoritative price; set KEEL_REVIEW_MODEL_PRICES "
                "or use a known model (fail closed rather than run an unbounded budget)"
            )
        return price.cost(prompt_tokens, completion_tokens)


__all__ = [
    "KNOWN_MODEL_PRICES",
    "ModelPrice",
    "PriceBook",
    "parse_price_overrides",
]
