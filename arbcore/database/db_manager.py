# arbcore/database/db_manager.py
# 数据库管理器 - PostgreSQL 专用
#
# IMPORTANT: This file MUST follow PostgreSQL SQL specification.
# See: arbcore/database/sql/SQL规范.md (Single source of truth)

import os
import threading
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from .adapters import PostgresAdapter
from .managers.fund_manager import FundManager
from .managers.market_manager import MarketManager
from .managers.system_manager import SystemManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    数据库管理器 - PostgreSQL 专用
    
    使用 PostgreSQL 标准 SQL：
    - 参数占位符：%s
    - 日期函数：now(), CURRENT_DATE
    - UPSERT：INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
    """
    
    def __init__(self, db_path=None, db_config=None):
        """
        初始化数据库管理器
        
        Args:
            db_path: 保留参数（兼容旧代码）
            db_config: PostgreSQL 配置字典
        """
        self.lock = threading.Lock()
        
        # 从配置文件读取数据库配置
        try:
            from arbcore.config.account_private import (
                PG_HOST, PG_PORT, PG_DB_NAME, PG_USER, PG_PASSWORD
            )
            self.pg_config = {
                "host": PG_HOST,
                "port": PG_PORT,
                "db_name": PG_DB_NAME,
                "user": PG_USER,
                "password": PG_PASSWORD
            }
        except ImportError:
            # 配置文件不存在，使用默认值
            logger.warning("未找到 account_private.py 配置文件，使用默认配置")
            self.pg_config = db_config or {
                "host": "localhost",
                "port": 5432,
                "db_name": "arb_master",
                "user": "postgres",
                "password": ""
            }
        
        # 直接创建 PostgreSQL 适配器
        logger.info(f"使用 PostgreSQL 数据库: {self.pg_config.get('host')}:{self.pg_config.get('port')}/{self.pg_config.get('db_name')}")
        self.adapter = PostgresAdapter(
            host=self.pg_config.get("host", "localhost"),
            port=self.pg_config.get("port", 5432),
            db_name=self.pg_config.get("db_name", "arb_master"),
            user=self.pg_config.get("user", "postgres"),
            password=self.pg_config.get("password", "")
        )
        
        # 初始化子 Manager（传入适配器）
        self.funds = FundManager(self.adapter, self.lock)
        self.market = MarketManager(self.adapter, self.lock)
        self.system = SystemManager(self.adapter, self.lock)
        
        # 创建 SQLAlchemy 引擎（供 pandas 使用）
        self._create_sqlalchemy_engine()
        
        # 初始化数据库表结构
        self.init_db()
        
        # 自动同步基金列表
        self._auto_sync_fund_list()
    
    def _get_conn(self):
        """获取数据库连接（兼容旧代码）"""
        return self.adapter.get_connection()
    
    def _create_sqlalchemy_engine(self):
        """创建 SQLAlchemy 引擎（供 pandas 使用）"""
        try:
            from urllib.parse import quote_plus
            password = quote_plus(self.pg_config.get("password", ""))
            conn_str = f"postgresql://{self.pg_config.get('user', 'postgres')}:{password}@{self.pg_config.get('host', 'localhost')}:{self.pg_config.get('port', 5432)}/{self.pg_config.get('db_name', 'arb_master')}"
            self._engine = create_engine(conn_str, poolclass=NullPool, echo=False)
            logger.info("SQLAlchemy 引擎创建成功")
        except Exception as e:
            logger.warning(f"SQLAlchemy 引擎创建失败: {e}")
            self._engine = None
    
    def get_sqlalchemy_engine(self):
        """获取 SQLAlchemy 引擎（供 pandas 使用）"""
        return self._engine
    
    def init_db(self):
        """初始化数据库表结构"""
        with self.lock:
            try:
                # 创建核心表
                self._create_core_tables()
                
                # 创建数据源配置表并初始化种子数据
                self._create_data_source_config_table()
                
                # 创建基金列表表
                self._create_fund_list_tables()
                
                # 创建其他辅助表
                self._create_auxiliary_tables()
                
                self.adapter.commit()
                logger.info("数据库表结构初始化完成")
            except Exception as e:
                logger.error(f"数据库初始化失败: {e}")
                self.adapter.rollback()
                raise
    
    def _create_core_tables(self):
        """创建核心数据表"""
        # fund_data 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_data (
                id SERIAL PRIMARY KEY,
                date TEXT,
                fund_code TEXT,
                price REAL,
                nav REAL,
                premium REAL,
                static_val REAL,
                val_error REAL,
                created_at TEXT
            )
        ''')
        self.adapter.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_fund_data_unique ON fund_data(date, fund_code)')
        
        # system_health 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS system_health (
                id SERIAL PRIMARY KEY,
                component TEXT NOT NULL,
                status TEXT,
                message TEXT,
                timestamp TIMESTAMP DEFAULT now()
            )
        ''')
        
        # exchange_rate 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS exchange_rate (
                date TEXT PRIMARY KEY,
                usd_cny_mid REAL,
                hkd_cny_mid REAL,
                updated_at TIMESTAMP DEFAULT now()
            )
        ''')
        
        # usa_etf_daily_prices 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS usa_etf_daily_prices (
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL,
                netvalue REAL,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, symbol)
            )
        ''')
        
        # index_daily 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS index_daily (
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, symbol)
            )
        ''')
        
        # futures_daily 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS futures_daily (
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                settle_price REAL,
                calibration REAL,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, symbol)
            )
        ''')
        
        # fund_basket_weights 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_basket_weights (
                date TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                underlying_symbol TEXT NOT NULL,
                weight REAL,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, fund_code, underlying_symbol)
            )
        ''')
        
        # fund_daily_factors 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_daily_factors (
                date TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                calibration REAL,
                hedge REAL,
                position REAL,
                nav REAL,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, fund_code)
            )
        ''')
        
        # raw_api_data 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS raw_api_data (
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                raw_content TEXT,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, source)
            )
        ''')
        
        # access_sync_status 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS access_sync_status (
                sync_date TEXT NOT NULL,
                access_source TEXT NOT NULL,
                sync_time TIMESTAMP DEFAULT now(),
                PRIMARY KEY (sync_date, access_source)
            )
        ''')
        
        # 创建索引
        self.adapter.execute('CREATE INDEX IF NOT EXISTS idx_fund_code_date ON fund_daily_factors (fund_code, date DESC)')
        self.adapter.execute('CREATE INDEX IF NOT EXISTS idx_health_component ON system_health(component)')
        self.adapter.execute('CREATE INDEX IF NOT EXISTS idx_etf_prices_date ON usa_etf_daily_prices(date DESC)')
        self.adapter.execute('CREATE INDEX IF NOT EXISTS idx_fund_basket ON fund_basket_weights(fund_code, date DESC)')
    
    def _create_data_source_config_table(self):
        """创建数据源配置表并初始化种子数据"""
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS data_source_config (
                id SERIAL PRIMARY KEY,
                module TEXT NOT NULL,
                source_name TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                config_json TEXT DEFAULT '{}',
                updated_at TIMESTAMP DEFAULT now()
            )
        ''')
        self.adapter.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_datasource_unique ON data_source_config(module, source_name)')
        
        # 检查是否需要初始化种子数据
        result = self.adapter.fetchone("SELECT COUNT(*) FROM data_source_config WHERE module = %s", ('realtime_market',))
        if result and result[0] == 0:
            initial_sources = [
                ('realtime_market', 'tdx', 1, 1, '{"desc": "通达信内存直连"}'),
                ('realtime_market', 'guojin', 2, 1, '{"desc": "国金QMT (xtquant)"}'),
                ('realtime_market', 'galaxy', 3, 1, '{"desc": "银河QMT (Socket)"}'),
                ('realtime_market', 'sina', 4, 1, '{"desc": "新浪财经轮询"}')
            ]
            for source in initial_sources:
                self.adapter.execute('''
                    INSERT INTO data_source_config (module, source_name, priority, is_active, config_json)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT idx_datasource_unique DO UPDATE SET
                    priority = EXCLUDED.priority, is_active = EXCLUDED.is_active
                ''', source)
            
            historical_sources = [
                ('historical_nav', 'eastmoney', 1, 1, '{"desc": "东方财富净值"}'),
                ('historical_price', 'sina', 1, 1, '{"desc": "新浪/腾讯价格"}')
            ]
            for source in historical_sources:
                self.adapter.execute('''
                    INSERT INTO data_source_config (module, source_name, priority, is_active, config_json)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT idx_datasource_unique DO UPDATE SET
                    priority = EXCLUDED.priority, is_active = EXCLUDED.is_active
                ''', source)
            
            logger.info("数据源配置种子数据已初始化")
    
    def _create_fund_list_tables(self):
        """创建基金列表相关表"""
        # unified_fund_list 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS unified_fund_list (
                category TEXT,
                fund_code TEXT PRIMARY KEY,
                fund_name TEXT,
                related_index TEXT,
                pos_ratio REAL DEFAULT 0.95,
                target_type TEXT DEFAULT 'ETF',
                idx_code TEXT,
                idx_name TEXT
            )
        ''')
        
        # fund_info 表（基金详细信息和状态）
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_info (
                fund_code TEXT PRIMARY KEY,
                fund_name TEXT,
                category TEXT,
                related_index TEXT,
                idx_name TEXT,
                pos_ratio REAL DEFAULT 0.95,
                purchase_status TEXT DEFAULT '开放',
                redemption_status TEXT DEFAULT '开放',
                purchase_fee REAL DEFAULT 0.0,
                redemption_fee REAL DEFAULT 0.0,
                target_type TEXT DEFAULT 'ETF'
            )
        ''')

# jsl_fund_list 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS jsl_fund_list (
                category TEXT,
                fund_code TEXT PRIMARY KEY,
                fund_name TEXT,
                related_index TEXT,
                pos_ratio REAL DEFAULT 0.95,
                target_type TEXT DEFAULT 'ETF'
            )
        ''')
        
        # [V11.0] 补齐 idx_code / idx_name 列（兼容旧版分享数据库）
        try:
            self.adapter.execute("ALTER TABLE unified_fund_list ADD COLUMN idx_code TEXT DEFAULT '-'")
        except Exception:
            pass
        try:
            self.adapter.execute("ALTER TABLE unified_fund_list ADD COLUMN idx_name TEXT DEFAULT '-'")
        except Exception:
            pass
        
        # index_history 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS index_history (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                close REAL,
                source TEXT,
                PRIMARY KEY (symbol, date)
            )
        ''')
        
        # index_realtime_quotes 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS index_realtime_quotes (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                last_price REAL,
                prev_close REAL,
                pct_change REAL,
                quote_time TEXT,
                source TEXT,
                updated_at TEXT
            )
        ''')
        
        # unified_fund_history 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS unified_fund_history (
                id SERIAL PRIMARY KEY,
                date TEXT NOT NULL,
                fund_code TEXT NOT NULL,
                price REAL,
                price_change REAL,
                nav REAL,
                nav_date TEXT,
                volume REAL,
                shares REAL,
                shares_added REAL,
                turnover_rate TEXT,
                static_val REAL,
                rt_val REAL,
                premium REAL,
                rt_premium REAL,
                index_close REAL,
                index_pct REAL,
                calibration REAL,
                purchase_status TEXT,
                redemption_status TEXT,
                created_at TIMESTAMP DEFAULT now()
            )
        ''')
        self.adapter.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_fund_history_unique ON unified_fund_history(date, fund_code)')
        
        # fund_purchase_status 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_purchase_status (
                fund_code TEXT PRIMARY KEY,
                purchase_status TEXT,
                redemption_status TEXT,
                purchase_fee TEXT,
                redemption_fee TEXT,
                purchase_limit REAL,
                updated_at TIMESTAMP DEFAULT now()
            )
        ''')
        try:
            self.adapter.execute("ALTER TABLE fund_purchase_status ADD COLUMN purchase_limit REAL")
        except Exception:
            pass
    
    def _create_auxiliary_tables(self):
        """创建辅助表"""
        # fund_intraday_quotes 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_intraday_quotes (
                id SERIAL PRIMARY KEY,
                fund_code TEXT NOT NULL,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                price REAL,
                rt_val REAL,
                premium REAL,
                created_at TIMESTAMP DEFAULT now()
            )
        ''')
        self.adapter.execute('CREATE INDEX IF NOT EXISTS idx_intraday_code_date ON fund_intraday_quotes(fund_code, date)')
        
        # user_trades 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS user_trades (
                id SERIAL PRIMARY KEY,
                fund_code TEXT NOT NULL,
                fund_name TEXT,
                account_suffix TEXT,
                action TEXT,
                volume INTEGER,
                price REAL,
                amount REAL,
                hedge_symbol TEXT,
                hedge_price REAL,
                hedge_vol INTEGER,
                fees REAL DEFAULT 0,
                trade_date TEXT DEFAULT CURRENT_DATE,
                remind_date TEXT,
                status TEXT DEFAULT 'ACTIVE',
                notes TEXT,
                created_at TIMESTAMP DEFAULT now()
            )
        ''')
        
        # fund_fees 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_fees (
                fund_code TEXT PRIMARY KEY,
                redemption_fee_rate REAL DEFAULT 0.5,
                commission_rate REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT now()
            )
        ''')
        
        # broker_redemption_fees 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS broker_redemption_fees (
                id SERIAL PRIMARY KEY,
                category TEXT,
                fund_code TEXT,
                broker_name TEXT,
                fee_rate TEXT,
                updated_at TIMESTAMP DEFAULT now()
            )
        ''')
        self.adapter.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_broker_fee_unique ON broker_redemption_fees(fund_code, broker_name)')
        
        # arbitrage_pairs 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS arbitrage_pairs (
                id SERIAL PRIMARY KEY,
                fund_code TEXT,
                fund_name TEXT,
                buy_date TEXT,
                buy_price REAL,
                buy_volume INTEGER,
                buy_amount REAL,
                buy_account TEXT,
                sell_date TEXT,
                sell_price REAL,
                sell_amount REAL,
                redemption_fee REAL DEFAULT 0,
                hedge_symbol TEXT,
                short_date TEXT,
                short_price REAL,
                short_volume INTEGER,
                short_amount REAL,
                cover_date TEXT,
                cover_price REAL,
                cover_amount REAL,
                us_commission REAL DEFAULT 0,
                pnl_rmb REAL,
                pnl_usd REAL,
                status TEXT DEFAULT 'ACTIVE',
                buy_notes TEXT,
                sell_notes TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            )
        ''')
        
        # etf_raw_api_data 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS etf_raw_api_data (
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                raw_content TEXT,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (date, source)
            )
        ''')
        
        # etf_rotation_list 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS etf_rotation_list (
                group_id INTEGER,
                lof_code TEXT,
                lof_name TEXT,
                etf_code TEXT,
                etf_name TEXT,
                track_index TEXT,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (lof_code, etf_code)
            )
        ''')
        
        # fund_watchlist 表
        self.adapter.execute('''
            CREATE TABLE IF NOT EXISTS fund_watchlist (
                fund_code TEXT PRIMARY KEY,
                added_at TIMESTAMP DEFAULT now()
            )
        ''')
    
    def _auto_sync_fund_list(self):
        """自动同步基金列表（如果表为空）"""
        try:
            result = self.adapter.fetchone("SELECT COUNT(*) FROM unified_fund_list")
            if result and result[0] == 0:
                # 从 fund_categories.json 加载基金列表
                config_path = os.path.join(
                    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')),
                    'arbcore', 'config', 'fund_categories.json'
                )

                if os.path.exists(config_path):
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    
                    fund_list = []
                    categories = config.get('categories', {})
                    
                    for cat_name, cat_data in categories.items():
                        funds = cat_data.get('funds', [])
                        for fund in funds:
                            fund_list.append({
                                'category': fund.get('category', cat_name),
                                'code': str(fund.get('code')),
                                'name': fund.get('name'),
                                'related_index': fund.get('trade_etf', '-'),
                                'target_type': fund.get('target_type', 'ETF')
                            })
                    
                    if fund_list:
                        self.sync_unified_fund_list(fund_list)
                        logger.info(f"自动同步基金列表完成，共 {len(fund_list)} 只基金")
                else:
                    logger.warning(f"基金配置文件不存在: {config_path}")
        except Exception as e:
            logger.warning(f"自动同步基金列表失败: {e}")
    
    # ==================== 代理方法 ====================
    # 将所有方法代理到对应的子 Manager
    
    def save_fund_data(self, *args, **kwargs): return self.funds.save_fund_data(*args, **kwargs)
    def update_fund_valuation(self, *args, **kwargs): return self.funds.update_fund_valuation(*args, **kwargs)
    def upsert_fund_factor(self, *args, **kwargs): return self.funds.upsert_fund_factor(*args, **kwargs)
    def update_fund_pos_ratio(self, *args, **kwargs): return self.funds.update_fund_pos_ratio(*args, **kwargs)
    def upsert_fund_basket_weight(self, *args, **kwargs): return self.funds.upsert_fund_basket_weight(*args, **kwargs)
    def get_latest_fund_factor(self, *args, **kwargs): return self.funds.get_latest_fund_factor(*args, **kwargs)
    def get_fund_basket(self, *args, **kwargs): return self.funds.get_fund_basket(*args, **kwargs)
    def get_latest_fund_price(self, *args, **kwargs): return self.funds.get_latest_fund_price(*args, **kwargs)
    def batch_save_fund_prices(self, *args, **kwargs): return self.funds.batch_save_fund_prices(*args, **kwargs)
    def sync_unified_fund_list(self, *args, **kwargs): return self.funds.sync_unified_fund_list(*args, **kwargs)
    def get_unified_fund_list(self, *args, **kwargs): return self.funds.get_unified_fund_list(*args, **kwargs)
    def sync_jsl_fund_list(self, *args, **kwargs): return self.funds.sync_jsl_fund_list(*args, **kwargs)
    def get_jsl_fund_list(self, *args, **kwargs): return self.funds.get_jsl_fund_list(*args, **kwargs)
    def batch_save_fund_purchase_status(self, *args, **kwargs): return self.funds.batch_save_fund_purchase_status(*args, **kwargs)
    def get_fund_purchase_status(self, *args, **kwargs): return self.funds.get_fund_purchase_status(*args, **kwargs)
    def save_unified_history(self, *args, **kwargs): return self.funds.save_unified_history(*args, **kwargs)

    def upsert_exchange_rate(self, *args, **kwargs): return self.market.upsert_exchange_rate(*args, **kwargs)
    def upsert_hkd_exchange_rate(self, *args, **kwargs): return self.market.upsert_hkd_exchange_rate(*args, **kwargs)
    def upsert_futures_daily(self, *args, **kwargs): return self.market.upsert_futures_daily(*args, **kwargs)
    def upsert_usa_etf_price(self, *args, **kwargs): return self.market.upsert_usa_etf_price(*args, **kwargs)

    def get_latest_usa_etf_date(self, *args, **kwargs): return self.market.get_latest_usa_etf_date(*args, **kwargs)
    def get_latest_futures_price(self, *args, **kwargs): return self.market.get_latest_futures_price(*args, **kwargs)
    def batch_save_futures_data(self, *args, **kwargs): return self.market.batch_save_futures_data(*args, **kwargs)

    def save_raw_api_data(self, *args, **kwargs): return self.system.save_raw_api_data(*args, **kwargs)
    def get_raw_api_data(self, *args, **kwargs): return self.system.get_raw_api_data(*args, **kwargs)
    def mark_access_synced(self, *args, **kwargs): return self.system.mark_access_synced(*args, **kwargs)
    def is_access_synced_today(self, *args, **kwargs): return self.system.is_access_synced_today(*args, **kwargs)
    def remove_access_sync_status(self, *args, **kwargs): return self.system.remove_access_sync_status(*args, **kwargs)
    def save_health_status(self, *args, **kwargs): return self.system.save_health_status(*args, **kwargs)
    def get_health_status(self, *args, **kwargs): return self.system.get_health_status(*args, **kwargs)
    def cleanup_old_data(self, *args, **kwargs): return self.system.cleanup_old_data(*args, **kwargs)
    def drop_deprecated_tables(self, *args, **kwargs): return self.system.drop_deprecated_tables(*args, **kwargs)
    def vacuum_database(self, *args, **kwargs): return self.system.vacuum_database(*args, **kwargs)
    def get_data_source_config(self, *args, **kwargs): return self.system.get_data_source_config(*args, **kwargs)
    def update_data_source_config(self, *args, **kwargs): return self.system.update_data_source_config(*args, **kwargs)

    # Compatibility methods
    def mark_api_synced(self, *args, **kwargs): return self.mark_access_synced(*args, **kwargs)
    def is_api_synced_today(self, *args, **kwargs): return self.is_access_synced_today(*args, **kwargs)
    
    def sync_etf_rotation_list(self, df):
        with self.lock:
            try:
                self.adapter.execute('DROP TABLE IF EXISTS etf_rotation_list')
                self.adapter.execute('''CREATE TABLE IF NOT EXISTS etf_rotation_list (group_id INTEGER, lof_code TEXT, lof_name TEXT, etf_code TEXT, etf_name TEXT, track_index TEXT, updated_at TIMESTAMP DEFAULT now(), PRIMARY KEY (lof_code, etf_code))''')
                for _, row in df.iterrows():
                    self.adapter.execute('INSERT INTO etf_rotation_list (group_id, lof_code, lof_name, etf_code, etf_name, track_index) VALUES (%s, %s, %s, %s, %s, %s)', (int(row['组别']), str(row['LOF基金代码']).split('.')[0].zfill(6), str(row['LOF基金名称']), str(row['ETF基金代码']).split('.')[0].zfill(6), str(row['ETF基金名称']), str(row['跟踪指数'])))
                self.adapter.commit()
                logger.info(f"Successfully synced {len(df)} rotation config items.")
            except Exception as e: logger.error(f"Failed to sync rotation config: {e}")
