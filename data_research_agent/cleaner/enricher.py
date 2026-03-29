"""Derived field computation for cleaned records."""

from __future__ import annotations

import operator
import re


# Supported operators for simple arithmetic expressions
OPS = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
}


def enrich_record(record: dict, rules: dict[str, str]) -> dict:
    """Apply enrichment rules to compute derived fields.

    Rules are simple arithmetic expressions involving two fields:
        "salary_mid": "(salary_min + salary_max) / 2"
        "price_diff": "price_new - price_old"

    Supports: +, -, *, / with two field references and optional parentheses.
    Falls back gracefully if fields are missing or non-numeric.

    Args:
        record: The data dict to enrich.
        rules: Mapping of new_field_name -> expression string.

    Returns:
        The record dict with new fields added (mutated in place).
    """
    for field_name, expression in rules.items():
        result = _evaluate_expression(expression, record)
        if result is not None:
            record[field_name] = result

    return record


def _evaluate_expression(expression: str, data: dict) -> float | None:
    """Evaluate a simple arithmetic expression against record data.

    Supports patterns like:
        "(field_a + field_b) / 2"
        "field_a - field_b"
        "field_a * 1.5"
    """
    # Clean up the expression
    expr = expression.strip()

    # Remove outer parentheses for the inner expression
    # Handle pattern: (expr) / N  or  (expr) * N
    outer_match = re.match(r"^\((.+)\)\s*([+\-*/])\s*(\d+\.?\d*)$", expr)
    if outer_match:
        inner_expr = outer_match.group(1)
        outer_op = outer_match.group(2)
        outer_val = float(outer_match.group(3))

        inner_result = _evaluate_binary(inner_expr, data)
        if inner_result is None:
            return None

        op_func = OPS.get(outer_op)
        if op_func and outer_val != 0:
            try:
                return round(op_func(inner_result, outer_val), 4)
            except (ZeroDivisionError, TypeError):
                return None
        return None

    # Simple binary expression: field_a OP field_b  or  field_a OP number
    return _evaluate_binary(expr, data)


def _evaluate_binary(expr: str, data: dict) -> float | None:
    """Evaluate a binary expression: operand OP operand."""
    # Remove parentheses
    expr = expr.strip().strip("()")

    for op_symbol in OPS:
        # Split on the operator, but not inside field names
        # Use regex to find the operator with spaces around it
        pattern = rf"\s*{re.escape(op_symbol)}\s*"
        parts = re.split(pattern, expr, maxsplit=1)

        if len(parts) == 2:
            left = _resolve_operand(parts[0].strip(), data)
            right = _resolve_operand(parts[1].strip(), data)

            if left is None or right is None:
                return None

            op_func = OPS[op_symbol]
            try:
                return round(op_func(left, right), 4)
            except (ZeroDivisionError, TypeError):
                return None

    # Single operand (field reference or number)
    return _resolve_operand(expr, data)


def _resolve_operand(token: str, data: dict) -> float | None:
    """Resolve a token to a numeric value — either a literal or a field reference."""
    token = token.strip()

    # Try as a number literal
    try:
        return float(token)
    except ValueError:
        pass

    # Try as a field reference
    value = data.get(token)
    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None
