{#- Reusable cleaning rules for the staging layer. -#}

{#- Supplier cost text -> DOUBLE.
    Arabic-Indic digits (٠-٩) become ASCII, the Arabic decimal separator (٫) and the decimal comma
    become a point, so '9,49' and '٩٫٤٩' both read as 9.49. Anything still not a number -> NULL
    (try_cast), which the quarantine then reports as invalid_cost. -#}
{% macro parse_cost(column) -%}
    try_cast(
        replace(translate(trim({{ column }}), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.'), ',', '.')
        as double
    )
{%- endmacro %}

{#- App timestamps are UTC text; Qafila counts days in Riyadh time (UTC+3, no daylight saving). -#}
{% macro to_riyadh(column) -%}
    ({{ column }}::timestamp + interval 3 hour)
{%- endmacro %}
