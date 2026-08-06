"""
SQL template rendering cho parameterized queries.
Thay thế {{ variable }} placeholders bằng giá trị thực.

Quy tắc: renderer KHÔNG tự thêm dấu nháy — template tự quản lý quoting.
Ví dụ template: DATE '{{ cob_dt }}' → DATE '2024-01-01'  (đúng)
Nếu renderer tự thêm quotes: DATE ''2024-01-01''            (sai)
"""

import re


def render_sql(sql_template: str, variables: dict) -> str:
    """
    Render SQL bằng cách thay thế {{ key }} placeholders bằng str(value).

    Template tự kiểm soát quoting — renderer không thêm dấu nháy.

    Args:
        sql_template: Chuỗi SQL có {{ variable }} placeholders
        variables:    Dict {key: value} để thay thế

    Returns:
        Chuỗi SQL đã render

    Raises:
        ValueError: Nếu còn placeholder chưa được thay thế
    """
    rendered = sql_template

    for key, value in variables.items():
        placeholder = "{{ " + key + " }}"
        rendered = rendered.replace(placeholder, str(value))

    unmatched = re.findall(r"\{\{\s*\w+\s*\}\}", rendered)
    if unmatched:
        raise ValueError(f"Placeholder chưa được thay thế trong SQL: {unmatched}")

    return rendered
