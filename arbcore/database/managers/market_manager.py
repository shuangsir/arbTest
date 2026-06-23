# arbcore/database/managers/market_manager.py
# 市场数据管理器
#
# IMPORTANT: This file MUST follow PostgreSQL SQL specification.
# See: arbcore/database/sql/SQL规范.md (Single source of truth)

from .base import BaseManager
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class MarketManager(BaseManager):
    """市场数据管理器"""
    
    def upsert_exchange_rate(self, date: str, usd_cny_mid: float = None, hkd_cny_mid: float = None):
        """更新汇率数据"""
        with self.lock:
            row = self.adapter.fetchone("SELECT usd_cny_mid, hkd_cny_mid FROM exchange_rate WHERE date = %s", (date,))
            
            exist_usd = row[0] if row else None
            exist_hkd = row[1] if row else None
            
            new_usd = usd_cny_mid if usd_cny_mid is not None else exist_usd
            new_hkd = hkd_cny_mid if hkd_cny_mid is not None else exist_hkd
            
            self.adapter.execute('''
                INSERT INTO exchange_rate (date, usd_cny_mid, hkd_cny_mid, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT(date) DO UPDATE SET
                usd_cny_mid = EXCLUDED.usd_cny_mid,
                hkd_cny_mid = EXCLUDED.hkd_cny_mid,
                updated_at = EXCLUDED.updated_at
            ''', (date, new_usd, new_hkd))
            self.adapter.commit()

    def upsert_futures_daily(self, date: str, symbol: str, settle_price: float = None, calibration: float = None, close_price: float = None):
        """更新期货每日数据"""
        with self.lock:
            # PostgreSQL 不支持 INSERT OR IGNORE，使用 ON CONFLICT DO NOTHING 代替
            self.adapter.execute('''
                INSERT INTO futures_daily (date, symbol, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT(date, symbol) DO NOTHING
            ''', (date, symbol))
            
            if settle_price is not None:
                self.adapter.execute("UPDATE futures_daily SET settle_price = %s, updated_at = now() WHERE date = %s AND symbol = %s", (settle_price, date, symbol))
            if calibration is not None:
                self.adapter.execute("UPDATE futures_daily SET calibration = %s, updated_at = now() WHERE date = %s AND symbol = %s", (calibration, date, symbol))
            if close_price is not None:
                self.adapter.execute("UPDATE futures_daily SET close_price = %s, updated_at = now() WHERE date = %s AND symbol = %s", (close_price, date, symbol))
            self.adapter.commit()
            
    def upsert_usa_etf_price(self, date: str, symbol: str, price: float, netvalue: float = None):
        """更新美股ETF价格"""
        with self.lock:
            # PostgreSQL 使用 UPSERT 语法
            self.adapter.execute('''
                INSERT INTO usa_etf_daily_prices (date, symbol, price, netvalue, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT(date, symbol) DO UPDATE SET
                price = EXCLUDED.price,
                netvalue = COALESCE(EXCLUDED.netvalue, usa_etf_daily_prices.netvalue),
                updated_at = EXCLUDED.updated_at
            ''', (date, symbol, price, netvalue))
            self.adapter.commit()


    def get_latest_usa_etf_date(self, symbol: str) -> str:
        """获取美股ETF最新日期"""
        result = self.adapter.fetchone("SELECT MAX(date) FROM usa_etf_daily_prices WHERE symbol = %s", (symbol,))
        return result[0] if result and result[0] else None

    def upsert_hkd_exchange_rate(self, date: str, hkd_cny_mid: float):
        """更新港币汇率"""
        self.upsert_exchange_rate(date, hkd_cny_mid=hkd_cny_mid)

    def get_latest_futures_price(self, symbol: str) -> Optional[float]:
        """获取最新期货价格"""
        try:
            result = self.adapter.fetchone('''
                SELECT settle_price FROM futures_daily 
                WHERE symbol = %s 
                ORDER BY date DESC LIMIT 1
            ''', (symbol,))
            return result[0] if result and result[0] is not None else None
        except Exception as e:
            logger.error(f"Failed to get futures price: {e}")
            return None

    def batch_save_futures_data(self, data_list: List[Dict[str, Any]]):
        """批量保存期货数据"""
        try:
            for data in data_list:
                date_str = data.get('date', datetime.now().strftime('%Y-%m-%d'))
                sym = data.get('symbol')
                price = data.get('price', data.get('settle_price'))
                self.upsert_futures_daily(date=date_str, symbol=sym, settle_price=price)
            logger.info(f"Batch saved futures data: {len(data_list)} items")
        except Exception as e:
            logger.error(f"Failed to batch save futures data: {e}")
