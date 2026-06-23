# arbcore/database/managers/fund_manager.py
# 基金数据管理器
#
# IMPORTANT: This file MUST follow PostgreSQL SQL specification.
# See: arbcore/database/sql/SQL规范.md (Single source of truth)

from .base import BaseManager
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class FundManager(BaseManager):
    """基金数据管理器"""
    
    def save_fund_data(self, date, fund_code, price, nav, premium):
        """保存基金数据"""
        with self.lock:
            # PostgreSQL UPSERT: ON CONFLICT ON CONSTRAINT
            self.adapter.execute('''
                INSERT INTO fund_data 
                (date, fund_code, price, nav, premium, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(date, fund_code) DO UPDATE SET
                price = EXCLUDED.price,
                nav = EXCLUDED.nav,
                premium = EXCLUDED.premium,
                created_at = EXCLUDED.created_at
            ''', (date, fund_code, price, nav, premium, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            self.adapter.commit()
            
    def update_fund_valuation(self, date: str, fund_code: str, static_val: float, val_error: float):
        """更新基金估值"""
        with self.lock:
            self.adapter.execute('''
                UPDATE fund_data 
                SET static_val = %s, val_error = %s
                WHERE date = %s AND fund_code = %s
            ''', (static_val, val_error, date, fund_code))
            self.adapter.commit()

    def upsert_fund_factor(self, date: str, fund_code: str, calibration: float, hedge: float, position: float, nav: float = None):
        """更新基金因子"""
        with self.lock:
            self.adapter.execute('''
                INSERT INTO fund_daily_factors 
                (date, fund_code, calibration, hedge, position, nav, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT(date, fund_code) DO UPDATE SET
                calibration = EXCLUDED.calibration,
                hedge = EXCLUDED.hedge,
                position = EXCLUDED.position,
                nav = EXCLUDED.nav,
                updated_at = EXCLUDED.updated_at
            ''', (date, fund_code, calibration, hedge, position, nav))
            self.adapter.commit()
            
    def update_fund_pos_ratio(self, fund_code: str, pos_ratio: float):
        """更新 unified_fund_list 的 pos_ratio（Woody API 获取的最新仓位同步到静态配置）"""
        if pos_ratio is None:
            return
        with self.lock:
            conn = self._get_conn()
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE unified_fund_list SET pos_ratio = %s
                    WHERE fund_code = %s
                ''', (pos_ratio, fund_code))
                if cursor.rowcount > 0:
                    logger.info(f"✅ pos_ratio 同步: {fund_code} → {pos_ratio*100:.2f}%")
                cursor.close()
            except Exception as e:
                logger.error(f"❌ 更新 {fund_code} pos_ratio 失败: {e}")
            
    def upsert_fund_basket_weight(self, date: str, fund_code: str, underlying_symbol: str, weight: float):
        """更新基金篮子权重"""
        with self.lock:
            self.adapter.execute('''
                INSERT INTO fund_basket_weights 
                (date, fund_code, underlying_symbol, weight, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT(date, fund_code, underlying_symbol) DO UPDATE SET
                weight = EXCLUDED.weight,
                updated_at = EXCLUDED.updated_at
            ''', (date, fund_code, underlying_symbol, weight))
            self.adapter.commit()

    def get_latest_fund_factor(self, fund_code: str):
        """获取最新的基金因子"""
        result = self.adapter.fetchone('''
            SELECT date, calibration, hedge, position
            FROM fund_daily_factors 
            WHERE fund_code = %s 
            ORDER BY date DESC LIMIT 1
        ''', (fund_code,))
        if result:
            return {
                "date": result[0], "calibration": result[1], 
                "hedge": result[2], "position": result[3]
            }
        return None

    def get_fund_basket(self, date: str, fund_code: str):
        """获取基金篮子"""
        results = self.adapter.fetchall('''
            SELECT underlying_symbol, weight 
            FROM fund_basket_weights 
            WHERE date = %s AND fund_code = %s
        ''', (date, fund_code))
        return [{"symbol": row[0], "weight": row[1]} for row in results]

    def get_latest_fund_price(self, code: str) -> Optional[Dict[str, Any]]:
        """获取最新的基金价格"""
        try:
            result = self.adapter.fetchone('''
                SELECT fund_code, price, nav, premium, created_at, date 
                FROM fund_data 
                WHERE fund_code = %s 
                ORDER BY date DESC LIMIT 1
            ''', (code,))
            if result:
                return {
                    'code': result[0],
                    'price': result[1],
                    'nav': result[2],
                    'premium': result[3],
                    'timestamp': result[4],
                    'date': result[5]
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get fund price: {e}")
            return None

    def batch_save_fund_prices(self, data_list: List[Dict[str, Any]]):
        """批量保存基金价格"""
        try:
            for data in data_list:
                date_str = data.get('date', datetime.now().strftime('%Y-%m-%d'))
                self.save_fund_data(
                    date=date_str, 
                    fund_code=data.get('code'), 
                    price=data.get('price'), 
                    nav=data.get('nav'), 
                    premium=data.get('premium')
                )
            logger.info(f"Batch saved fund prices: {len(data_list)} items")
        except Exception as e:
            logger.error(f"Failed to batch save fund prices: {e}")
            
    def sync_jsl_fund_list(self, fund_list: List[Dict[str, str]]):
        """同步集思录基金列表"""
        with self.lock:
            try:
                for item in fund_list:
                    self.adapter.execute('''
                        INSERT INTO jsl_fund_list 
                        (category, fund_code, fund_name, related_index)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT(fund_code) DO UPDATE SET
                        category = EXCLUDED.category,
                        fund_name = EXCLUDED.fund_name,
                        related_index = EXCLUDED.related_index
                    ''', (item['category'], item['code'], item['name'], item.get('related_index', '-')))
                self.adapter.commit()
                logger.info(f"Successfully synced {len(fund_list)} JSL items to database.")
            except Exception as e:
                logger.error(f"Failed to sync JSL fund list: {e}")

    def get_jsl_fund_list(self) -> List[Dict[str, str]]:
        """获取集思录基金列表"""
        results = self.adapter.fetchall('''
            SELECT category, fund_code, fund_name, related_index 
            FROM jsl_fund_list
        ''')
        return [{"category": r[0], "code": r[1], "name": r[2], "related_index": r[3]} for r in results]

    def batch_save_fund_purchase_status(self, df):
        """批量保存基金申赎状态"""
        with self.lock:
            try:
                records = df.to_records(index=False)
                for record in records:
                    self.adapter.execute('''
                        INSERT INTO fund_purchase_status 
                        (fund_code, purchase_status, redemption_status, purchase_fee, redemption_fee, updated_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        ON CONFLICT(fund_code) DO UPDATE SET
                        purchase_status = EXCLUDED.purchase_status,
                        redemption_status = EXCLUDED.redemption_status,
                        purchase_fee = EXCLUDED.purchase_fee,
                        redemption_fee = EXCLUDED.redemption_fee,
                        updated_at = EXCLUDED.updated_at
                    ''', tuple(record))
                self.adapter.commit()
                logger.info(f"Successfully cached {len(df)} fund purchase status items!")
            except Exception as e:
                logger.error(f"Failed to batch save fund purchase status: {e}")

    def get_fund_purchase_status(self, fund_code: str) -> Dict[str, str]:
        """获取基金申赎状态"""
        result = self.adapter.fetchone('''
            SELECT purchase_status, redemption_status, purchase_fee, redemption_fee
            FROM fund_purchase_status WHERE fund_code = %s
        ''', (fund_code,))
        if result:
            return {
                'purchase_status': result[0], 'redemption_status': result[1],
                'purchase_fee': result[2], 'redemption_fee': result[3]
            }
        return {
            'purchase_status': '未知', 'redemption_status': '未知',
            'purchase_fee': '0%', 'redemption_fee': '0.50%'
        }

    def sync_unified_fund_list(self, fund_list: List[Dict[str, Any]]):
        """同步统一基金列表"""
        with self.lock:
            try:
                for item in fund_list:
                    self.adapter.execute('''
                        INSERT INTO unified_fund_list
                        (category, fund_code, fund_name, related_index, pos_ratio, target_type)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT(fund_code) DO UPDATE SET
                        category = EXCLUDED.category,
                        fund_name = EXCLUDED.fund_name,
                        related_index = EXCLUDED.related_index,
                        pos_ratio = EXCLUDED.pos_ratio,
                        target_type = EXCLUDED.target_type
                    ''', (
                        item['category'], 
                        item['code'], 
                        item['name'], 
                        item.get('related_index', '-'), 
                        item.get('pos_ratio', 0.95), 
                        item.get('target_type', 'ETF')
                    ))
                self.adapter.commit()
                logger.info(f"Successfully synced {len(fund_list)} unified items to database.")
            except Exception as e:
                logger.error(f"Failed to sync unified fund list: {e}")

    def get_unified_fund_list(self) -> List[Dict[str, Any]]:
        """获取统一基金列表"""
        results = self.adapter.fetchall('''
            SELECT category, fund_code, fund_name, related_index, pos_ratio, target_type 
            FROM unified_fund_list
        ''')
        return [
            {
                "category": r[0], 
                "code": r[1], 
                "name": r[2], 
                "related_index": r[3], 
                "pos_ratio": r[4], 
                "target_type": r[5]
            } 
            for r in results
        ]

    def save_unified_history(self, date_str, fund_code, **kwargs):
        """
        保存统一基金历史数据
        支持动态列更新，自动处理 NULL 覆盖问题。
        """
        with self.lock:
            try:
                # 过滤掉 None 值，避免覆盖已有数据
                valid_data = {k: v for k, v in kwargs.items() if v is not None}
                if not valid_data: return

                cols = ['date', 'fund_code'] + list(valid_data.keys())
                placeholders = ['%s'] * len(cols)
                vals = [date_str, fund_code] + list(valid_data.values())

                # 使用 PostgreSQL 的 UPSERT 语法
                update_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in valid_data.keys()])

                query = f'''
                    INSERT INTO unified_fund_history ({", ".join(cols)})
                    VALUES ({", ".join(placeholders)})
                    ON CONFLICT(date, fund_code) DO UPDATE SET
                    {update_clause}
                '''
                self.adapter.execute(query, vals)
                self.adapter.commit()
            except Exception as e:
                logger.error(f"Failed to save unified history for {fund_code}: {e}")
