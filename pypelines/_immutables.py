class FrozenError(TypeError):
    pass


class Frozen:
    frozen: bool = False

    def __init__(self):
        self.frozen = True

    def __setattr__(self, key, value):
        if self.frozen:
            raise FrozenError(f"{self} is frozen and cannot be written to.")

        object.__setattr__(self, key, value)


__all__ = ("Frozen", "FrozenError")
