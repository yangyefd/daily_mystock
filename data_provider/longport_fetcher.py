# -*- coding: utf-8 -*-
"""
===================================
LongportFetcher - 主力数据源 (Priority 0)
===================================

数据来源：LongPort Open API (长桥)
特点：实时性好、支持多市场、需要 App Key/Secret
优点：数据质量极高、支持港美A股

流控策略：
1. 实现"每分钟调用计数器"
2. 超过配额（默认限制）时，强制休眠到下一分钟
3. 使用 tenacity 实现指数退避重试
"""

import logging
import time
from datetime import datetime, date
from typing import Optional, List, TYPE_CHECKING

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

if TYPE_CHECKING:
    from .akshare_fetcher import RealtimeQuote

try:
    from longport.openapi import (
        QuoteContext, 
        Config as LongportConfig, 
        Period,
        AdjustType,
    )
except ImportError:
    pass  # 在 _init_api 中处理导入错误

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS
from config import get_config

logger = logging.getLogger(__name__)


class LongportFetcher(BaseFetcher):
    """
    LongPort 数据源实现
    
    优先级：0
    数据来源：LongPort OpenAPI
    
    关键策略：
    - 每分钟调用计数器，模拟流控
    - 失败后指数退避重试
    """
    
    name = "LongportFetcher"
    priority = 0  # 最高优先级，排在 EfinanceFetcher 之前
    
    def __init__(self, rate_limit_per_minute: int = 60):
        """
        初始化 LongportFetcher
        
        Args:
            rate_limit_per_minute: 每分钟最大请求数（保守设为60）
        """
        self.rate_limit_per_minute = rate_limit_per_minute
        self._call_count = 0  # 当前分钟内的调用次数
        self._minute_start: Optional[float] = None  # 当前计数周期开始时间
        self._ctx: Optional[object] = None  # LongPort Context 实例
        
        # 尝试初始化 API
        self._init_api()
    
    def _init_api(self) -> None:
        """
        初始化 LongPort API Context
        
        需要配置 lb_app_key, lb_app_secret, lb_access_token
        """
        config = get_config()
        
        # 假设 config 中有这些字段，请根据实际 config.py 调整属性名
        app_key = getattr(config, 'longport_app_key', None)
        app_secret = getattr(config, 'longport_app_secret', None)
        access_token = getattr(config, 'longport_access_token', None)
        
        if not all([app_key, app_secret, access_token]):
            logger.warning("LongPort 配置不完整 (app_key/secret/token)，此数据源不可用")
            return
        
        try:
            # 配置 LongPort
            lp_config = LongportConfig(app_key, app_secret, access_token)
            self._ctx = QuoteContext(lp_config)
            
            logger.info("LongPort API 初始化成功")
            
        except ImportError:
            logger.error("未安装 longport SDK，请运行: pip install longport")
            self._ctx = None
        except Exception as e:
            logger.error(f"LongPort API 初始化失败: {e}")
            self._ctx = None
            
    def _check_rate_limit(self) -> None:
        """
        检查并执行速率限制 (与 Tushare 逻辑保持完全一致)
        """
        current_time = time.time()
        
        # 检查是否需要重置计数器（新的一分钟）
        if self._minute_start is None:
            self._minute_start = current_time
            self._call_count = 0
        elif current_time - self._minute_start >= 60:
            # 已经过了一分钟，重置计数器
            self._minute_start = current_time
            self._call_count = 0
            logger.debug("速率限制计数器已重置")
        
        # 检查是否超过配额
        if self._call_count >= self.rate_limit_per_minute:
            # 计算需要等待的时间（到下一分钟）
            elapsed = current_time - self._minute_start
            sleep_time = max(0, 60 - elapsed) + 1  # +1 秒缓冲
            
            logger.warning(
                f"LongPort 达到速率限制 ({self._call_count}/{self.rate_limit_per_minute} 次/分钟)，"
                f"等待 {sleep_time:.1f} 秒..."
            )
            
            time.sleep(sleep_time)
            
            # 重置计数器
            self._minute_start = time.time()
            self._call_count = 0
        
        # 增加调用计数
        self._call_count += 1
        logger.debug(f"LongPort 当前分钟调用次数: {self._call_count}/{self.rate_limit_per_minute}")

    def _convert_stock_code(self, stock_code: str) -> str:
        """
        转换股票代码为 LongPort 格式
        
        LongPort 格式：
        - 沪市：600519.SH
        - 深市：000001.SZ
        - 沪市ETF：510210.SH
        - 深市ETF：159320.SZ
        - 港股：00700.HK
        - 美股：AAPL.US
        """
        code = stock_code.strip().upper()
        
        # 已经包含后缀的情况
        if '.' in code:
            return code
        
        # === A 股主板 ===
        # 沪市主板：600xxx, 601xxx, 603xxx
        # 沪市科创板：688xxx
        if code.startswith(('600', '601', '603', '688')):
            return f"{code}.SH"
        
        # 深市主板：000xxx
        # 深市中小板：002xxx
        # 深市创业板：300xxx, 301xxx
        elif code.startswith(('000', '002', '300', '301')):
            return f"{code}.SZ"
        
        # === ETF 基金 ===
        # 沪市 ETF：510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 516xxx, 517xxx, 518xxx, 560xxx, 561xxx, 562xxx, 563xxx
        elif code.startswith(('51', '56')):

            return f"{code}.SH"
        
        # 深市 ETF：159xxx
        elif code.startswith('159'):
            return f"{code}.SZ"
        
        # === LOF 基金 ===
        # 沪市 LOF：501xxx
        elif code.startswith('501'):
            return f"{code}.SH"
        
        # 深市 LOF：16xxxx
        elif code.startswith('16'):
            return f"{code}.SZ"
        
        # === 可转债 ===
        # 沪市可转债：110xxx, 113xxx
        elif code.startswith(('110', '113')):

            return f"{code}.SH"
        
        # 深市可转债：123xxx, 127xxx, 128xxx
        elif code.startswith(('123', '127', '128')):

            return f"{code}.SZ"
        
        # === 港股（5位数字）===

        elif len(code) == 5 and code.isdigit():
            return f"{code}.HK"
        
        # === 美股（纯字母）===

        elif code.isalpha():
            return f"{code}.US"
        
        else:
            logger.warning(f"无法确定股票 {code} 的市场，默认使用深市 SZ")
            return f"{code}.SZ"

    def get_realtime_quote(self, stock_code: str) -> Optional['RealtimeQuote']:
        """
        获取实时行情数据
        """
        try:
            symbol = self._convert_stock_code(stock_code)
            
            quotes = self._ctx.quote([symbol])
            
            if not quotes:
                logger.warning(f"LongPort 未返回 {stock_code} 的实时行情")
                return None
            
            q = quotes[0]
            
            # 安全获取股票名称
            stock_name = ""
            for attr in ['name', 'symbol_name', 'security_name']:
                if hasattr(q, attr):
                    stock_name = getattr(q, attr, "")
                    break
            
            # 如果还是没有名称，尝试从 static_info 获取
            if not stock_name:
                try:
                    static_info = self._ctx.static_info([symbol])
                    if static_info:
                        stock_name = getattr(static_info[0], 'name_cn', '') or getattr(static_info[0], 'name', '')
                except Exception:
                    pass
            
            # 安全获取各个字段
            def safe_get(obj, *attrs, default=0.0):
                """安全获取对象属性，支持多个候选属性名"""
                for attr in attrs:
                    if hasattr(obj, attr):
                        val = getattr(obj, attr, None)
                        if val is not None:
                            try:
                                return float(val)
                            except (TypeError, ValueError):
                                return val
                return default
            
            from .akshare_fetcher import RealtimeQuote
            
            # 严格按照 RealtimeQuote 定义的字段来构造
            return RealtimeQuote(
                code=stock_code,
                name=stock_name or stock_code,
                price=safe_get(q, 'last_done', 'last_price', 'current_price'),
                change_pct=safe_get(q, 'change_rate', 'change_pct', 'pct_change'),
                change_amount=safe_get(q, 'change_val', 'change_amount', 'change'),
                volume_ratio=safe_get(q, 'volume_ratio'),
                turnover_rate=safe_get(q, 'turnover_rate'),
                amplitude=safe_get(q, 'amplitude'),
                pe_ratio=safe_get(q, 'pe_ttm', 'pe_ratio'),
                pb_ratio=safe_get(q, 'pb_ratio', 'pb'),
                total_mv=safe_get(q, 'total_market_value', 'market_cap'),
                circ_mv=safe_get(q, 'circulating_market_value', 'float_market_value'),
                change_60d=0.0,  # LongPort 实时接口通常不提供此字段
                high_52w=safe_get(q, 'high_52w', 'week52_high'),
                low_52w=safe_get(q, 'low_52w', 'week52_low'),
            )
            
        except Exception as e:
            logger.error(f"LongPort 获取实时行情失败 {stock_code}: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((Exception)), # 捕获宽泛异常，因为 LongPort 网络错误类型多样
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 LongPort 获取历史 K 线数据
        """
        try:
            symbol = self._convert_stock_code(stock_code)
            
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # 修复：兼容不同版本的 AdjustType 枚举
            # 尝试多种可能的枚举值名称
            adjust_type = None
            for attr_name in ['ForwardAdjust', 'Forward', 'FORWARD_ADJUST', 'FORWARD']:
                if hasattr(AdjustType, attr_name):
                    adjust_type = getattr(AdjustType, attr_name)
                    logger.debug(f"使用复权类型: AdjustType.{attr_name}")
                    break
            
            # 如果都没找到，使用不复权作为兜底
            if adjust_type is None:
                logger.warning("无法找到前复权枚举，使用不复权模式")
                adjust_type = AdjustType.NoAdjust
            
            candlesticks = self._ctx.history_candlesticks_by_date(
                symbol=symbol,
                period=Period.Day,
                adjust_type=adjust_type,
                start=start_dt,
                end=end_dt,
            )
            
            # 转换为 DataFrame
            data_list = []
            for candle in candlesticks:
                data_list.append({
                    'date': candle.timestamp,  # LongPort 返回的是 datetime 对象
                    'open': float(candle.open),
                    'high': float(candle.high),
                    'low': float(candle.low),
                    'close': float(candle.close),
                    'volume': int(candle.volume),
                    'amount': float(candle.turnover)
                })
            
            df = pd.DataFrame(data_list)
            return df
            
        except Exception as e:
            raise DataFetchError(f"LongPort 获取数据失败: {e}")
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 LongPort 数据
        
        Need to ensure:
        date, open, high, low, close, volume, amount, pct_chg
        """
        if df.empty:
            return pd.DataFrame(columns=['code'] + STANDARD_COLUMNS)

        df = df.copy()
        
        # 1. 处理日期格式
        # LongPort timestamp 通常已是 datetime 或 date 对象
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # 2. 计算 pct_chg (涨跌幅)
        # LongPort history 通常不直接返回涨跌幅，需要计算
        if 'pct_chg' not in df.columns and 'close' in df.columns:
            # 计算公式: (今日收盘 - 昨日收盘) / 昨日收盘 * 100
            # 注意：这会导致第一行数据为 NaN，因为没有前一日数据
            df['pct_chg'] = df['close'].pct_change() * 100
            # 填充 NaN 为 0 (或者丢弃第一行，取决于业务需求，这里选择填0保持行数)
            df['pct_chg'] = df['pct_chg'].fillna(0)
            
        # 3. 确保数值类型正确
        numeric_cols = ['open', 'high', 'low', 'close', 'amount', 'pct_chg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 4. 添加代码列
        df['code'] = stock_code
        
        # 5. 只保留标准列
        keep_cols = ['code'] + STANDARD_COLUMNS
        # 确保所有标准列都存在，不存在的补 NaN
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
                
        df = df[keep_cols]
        
        # 按日期排序
        df = df.sort_values('date').reset_index(drop=True)
        
        return df

if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    # 注意：需要确保 config.py 中有 LongPort 的配置
    fetcher = LongportFetcher()
    
    try:
        # 尝试获取茅台数据
        df = fetcher.get_daily_data('600519', start_date='2023-01-01', end_date='2023-01-10')
        print(f"获取成功，共 {len(df)} 条数据")
        print(df)
    except Exception as e:
        print(f"获取失败: {e}")
