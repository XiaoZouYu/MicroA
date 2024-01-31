from mia.containers import ServiceContainer


def get_script(container: "ServiceContainer", script_cls, **match_attrs):
    """ 获取对应的实例
    """
    for ext in container.scripts:
        if isinstance(ext, script_cls):
            if not match_attrs:
                return ext

            def has_attribute(name, value):
                return getattr(ext, name) == value

            if all([has_attribute(name, value) for name, value in match_attrs.items()]):
                return ext
