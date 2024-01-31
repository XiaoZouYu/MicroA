from pydoc import locate


def import_from_path(path: str):
    """
    通过路径字符串导入模块或对象。

    Parameters:
        path (str): 要导入的路径字符串。

    Returns:
        obj: 成功导入的模块或对象。

    Raises:
        ImportError: 导入失败时抛出的异常，包含详细的错误信息。
    """
    if path is None:
        # 如果路径为 None，直接返回，不进行导入操作
        return

    try:
        # 尝试通过路径字符串导入模块或对象
        obj = locate(path)
        if obj is None:
            # 如果 locate 返回 None，表示导入失败，抛出 ImportError 异常
            raise ImportError(f"无法导入 `{path}`，请检查路径格式是否正确！")
        return obj
    except Exception as e:
        # 捕获其他可能的异常，并将详细信息添加到 ImportError
        raise ImportError(f"导入 `{path}` 时发生错误：{str(e)}") from e
