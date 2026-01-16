"""
Conectores para diferentes brokers (Strategy Pattern)
"""

from .base_strategy import BaseBrokerStrategy
from .bull_market import BullMarketStrategy
from .balanz import BalanzStrategy

__all__ = ['BaseBrokerStrategy', 'BullMarketStrategy', 'BalanzStrategy']
