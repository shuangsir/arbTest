# arbcore/database/managers/system_manager.py
# 系统数据管理器
#
# IMPORTANT: This file MUST follow PostgreSQL SQL specification.
# See: arbcore/database/sql/SQL规范.md (Single source of truth)

from .base import BaseManager
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class SystemManager(BaseManager):
    """系统数据管理器"""
    
    def save_raw_api_data(self, date: str, source: str, raw_content: str):
        """保存原始API数据"""
        with self.lock:
            self.adapter.execute('''
                INSERT INTO raw_api_data (date, source, raw_content, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT(date, source) DO UPDATE SET
                raw_content = EXCLUDED.raw_content,
                updated_at = EXCLUDED.updated_at
            ''', (date, source, raw_content))
            self.adapter.commit()

    def get_raw_api_data(self, date: str, source: str):
        """获取原始API数据"""
        result = self.adapter.fetchone("SELECT raw_content FROM raw_api_data WHERE date = %s AND source = %s", (date, source))
        return result[0] if result else None

    def mark_access_synced(self, sync_date: str, source: str):
        """标记同步状态"""
        with self.lock:
            self.adapter.execute('''
                INSERT INTO access_sync_status (sync_date, access_source, sync_time)
                VALUES (%s, %s, now())
                ON CONFLICT(sync_date, access_source) DO UPDATE SET
                sync_time = EXCLUDED.sync_time
            ''', (sync_date, source))
            self.adapter.commit()
            
    def is_access_synced_today(self, sync_date: str, source: str) -> bool:
        """检查是否已同步"""
        result = self.adapter.fetchone("SELECT 1 FROM access_sync_status WHERE sync_date = %s AND access_source = %s", (sync_date, source))
        return result is not None

    def save_health_status(self, component: str, status: str, message: str = ""):
        """保存健康状态"""
        with self.lock:
            try:
                self.adapter.execute('''
                    INSERT INTO system_health (component, status, message, timestamp)
                    VALUES (%s, %s, %s, now())
                ''', (component, status, message))
                self.adapter.commit()
            except Exception as e:
                logger.error(f"Failed to save health status: {e}")

    def get_health_status(self, component: str = None) -> List[Dict[str, Any]]:
        """获取健康状态"""
        try:
            if component:
                results = self.adapter.fetchall('''
                    SELECT component, status, message, timestamp 
                    FROM system_health 
                    WHERE component = %s 
                    ORDER BY timestamp DESC LIMIT 10
                ''', (component,))
            else:
                results = self.adapter.fetchall('''
                    SELECT component, status, message, timestamp 
                    FROM system_health 
                    ORDER BY timestamp DESC LIMIT 50
                ''')
            return [
                {'component': row[0], 'status': row[1], 'message': row[2], 'timestamp': row[3]}
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to get health status: {e}")
            return []

    def remove_access_sync_status(self, sync_date: str, source: str):
        """删除同步状态"""
        with self.lock:
            self.adapter.execute("DELETE FROM access_sync_status WHERE sync_date = %s AND access_source = %s", (sync_date, source))
            self.adapter.commit()

    def cleanup_old_data(self, days: int = 30):
        """清理旧数据"""
        with self.lock:
            try:
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                
                self.adapter.execute('DELETE FROM futures_daily WHERE date < %s', (cutoff_date,))
                self.adapter.execute('DELETE FROM usa_etf_daily_prices WHERE date < %s', (cutoff_date,))
                self.adapter.execute('DELETE FROM fund_data WHERE date < %s', (cutoff_date,))
                self.adapter.execute('DELETE FROM system_health WHERE timestamp < %s', (cutoff_date,))
                
                sync_cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                self.adapter.execute('DELETE FROM access_sync_status WHERE sync_date < %s', (sync_cutoff,))
                
                self.adapter.commit()
                logger.info(f"Cleaned up old data, kept last {days} days")
            except Exception as e:
                logger.error(f"Failed to cleanup old data: {e}")

    def drop_deprecated_tables(self):
        """删除废弃表"""
        with self.lock:
            try:
                # PostgreSQL 使用 information_schema 查询表
                results = self.adapter.fetchall('''
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name LIKE 'fund_history_%%'
                ''')
                old_tables = [row[0] for row in results]
                if old_tables:
                    logger.info(f"Found {len(old_tables)} old tables, dropping...")
                    for table in old_tables:
                        self.adapter.execute(f"DROP TABLE IF EXISTS {table}")
                    self.adapter.commit()
                    logger.info("Old tables dropped.")
            except Exception as e:
                logger.error(f"Failed to drop old tables: {e}")

    def vacuum_database(self):
        """清理数据库（PostgreSQL 使用 VACUUM）"""
        try:
            self.adapter.execute('VACUUM')
            self.adapter.commit()
            logger.info("Database VACUUM completed")
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")

    def get_data_source_config(self, module: str) -> List[Dict[str, Any]]:
        """获取指定模块的数据源配置，按优先级排序"""
        try:
            results = self.adapter.fetchall('''
                SELECT source_name, priority, is_active, config_json 
                FROM data_source_config 
                WHERE module = %s AND is_active = 1
                ORDER BY priority ASC
            ''', (module,))
            return [
                {
                    'source_name': row[0],
                    'priority': row[1],
                    'is_active': row[2],
                    'config_json': row[3]
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to get data source config for {module}: {e}")
            return []

    def update_data_source_config(self, module: str, source_name: str, priority: int = None, is_active: int = None, config_json: str = None):
        """更新数据源配置"""
        with self.lock:
            try:
                updates = []
                params = []
                if priority is not None:
                    updates.append("priority = %s")
                    params.append(priority)
                if is_active is not None:
                    updates.append("is_active = %s")
                    params.append(is_active)
                if config_json is not None:
                    updates.append("config_json = %s")
                    params.append(config_json)
                
                if not updates: return
                
                params.extend([module, source_name])
                self.adapter.execute(f'''
                    UPDATE data_source_config 
                    SET {', '.join(updates)}, updated_at = now()
                    WHERE module = %s AND source_name = %s
                ''', params)
                self.adapter.commit()
                logger.info(f"Updated data source config: {module} -> {source_name}")
            except Exception as e:
                logger.error(f"Failed to update data source config: {e}")
