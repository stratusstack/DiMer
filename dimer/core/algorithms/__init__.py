"""Strategy-pattern algorithm classes for DiMer table comparison."""

from dimer.core.algorithms.base import BaseAlgorithm
from dimer.core.algorithms.bisection import BisectionAlgorithm
from dimer.core.algorithms.cross_db import CrossDbDiffAlgorithm
from dimer.core.algorithms.hash_diff import HashDiffAlgorithm
from dimer.core.algorithms.join_diff import JoinDiffAlgorithm
from dimer.core.algorithms.sampled import SampledAlgorithm

__all__ = [
    "BaseAlgorithm",
    "BisectionAlgorithm",
    "CrossDbDiffAlgorithm",
    "HashDiffAlgorithm",
    "JoinDiffAlgorithm",
    "SampledAlgorithm",
]
