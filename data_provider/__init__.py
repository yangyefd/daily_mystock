# -*- coding: utf-8 -*-
"""
===================================
数据源策略层 - 包初始化
===================================

本包实现策略模式管理多个数据源，实现：
1. 统一的数据获取接口
2. 自动故障切换
3. 防封禁流控策略

数据源优先级：
1. longport_fetcher.py (Priority 0) - 主力数据源，来自 LongPort OpenAPI
2. EfinanceFetcher (Priority 1) - 备用数据源1，来自 efinance 库
3. AkshareFetcher (Priority 2) - 来自 akshare 库
4. TushareFetcher (Priority 3) - 来自 tushare 库
5. BaostockFetcher (Priority 4) - 来自 baostock 库
6. YfinanceFetcher (Priority 5) - 来自 yfinance 库
"""

from .base import BaseFetcher, DataFetcherManager
from .longport_fetcher import LongportFetcher
from .efinance_fetcher import EfinanceFetcher
from .akshare_fetcher import AkshareFetcher
from .tushare_fetcher import TushareFetcher
from .baostock_fetcher import BaostockFetcher
from .yfinance_fetcher import YfinanceFetcher

__all__ = [
    'BaseFetcher',
    'DataFetcherManager',
    'LongportFetcher',
    'EfinanceFetcher',
    'AkshareFetcher',
    'TushareFetcher',
    'BaostockFetcher',
    'YfinanceFetcher',
]
