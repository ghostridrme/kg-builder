"""Shared setup extracted from test_recovery; assertions stay in owner tests."""

class Store:
    def __init__(self):
        self.calls = []
    def compare_and_set(self, key, **kwargs):
        self.calls.append((key, kwargs))

