def sanitize_sql(sql: str):
    return (
        sql.replace('sql: "', "")
        .replace("\\n", " ")
        .replace("\\", "")
        .replace("  ", "")
        .replace("`", "")
        .strip()
    )
