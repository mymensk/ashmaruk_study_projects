def merge_dfs(base_df, dfs, key='user_id', how='left'):
    result = base_df.copy()

    for df in dfs:
        result = result.merge(df, on=key, how=how)

    return result
