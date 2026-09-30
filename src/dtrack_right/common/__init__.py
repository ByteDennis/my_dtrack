import functools


# >>> make a class or function refuse to run until the reason is fixed; remove the decorator to re-enable <<< #
def disabled(reason: str):
    def wrap(target):
        def refuse(*args, **_kwargs):
            name = args[0].__name__ if isinstance(target, type) else target.__qualname__
            raise NotImplementedError(f"{name} is disabled: {reason}")

        if isinstance(target, type):
            target.__new__ = refuse
            return target
        return functools.wraps(target)(refuse)

    return wrap
