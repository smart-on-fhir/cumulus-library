""" abstract base for python-based study executors """
from abc import ABC, abstractmethod


class BaseRunner(ABC):
    """Generic base class for python study runners.

    To use a python runner, extend this class exactly once in a new module.
    See ./studies/vocab for an example usage.
    """

    @abstractmethod
    def run_executor(self, cursor: object, schema: str, verbose: bool):
        """Main entrypoint for python runners

        :param cursor: A PEP-249 compatible cursor
        :param schema: A schema name
        :param verbose: toggle for verbose output mode
        """
        raise NotImplementedError
