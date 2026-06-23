# arbcore/database/adapters/postgres_adapter.py
# PostgreSQL 数据库适配器 - 线程安全版本
#
# IMPORTANT: This file MUST follow PostgreSQL SQL specification.
# See: arbcore/database/sql/SQL规范.md (Single source of truth)

import psycopg2
import psycopg2.extras
import logging
import threading
from typing import List, Tuple, Optional, Any
from .base import IDatabaseAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(IDatabaseAdapter):
    """
    PostgreSQL 数据库适配器
    
    线程安全设计：每个线程使用独立连接
    - 使用 threading.local() 存储线程本地连接
    - get_connection() 返回当前线程的连接
    
    注意：SQL 语法必须符合 PostgreSQL 标准
    - 参数占位符：%s
    - 日期函数：now(), CURRENT_DATE
    - UPSERT：INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
    """
    
    def __init__(self, host: str, port: int, db_name: str, user: str, password: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self._local = threading.local()
        self._connected = False
    
    def _create_connection(self):
        """创建新连接"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.user,
                password=self.password
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                logger.info(f"数据库 {self.db_name} 不存在，尝试创建...")
                if self._create_database():
                    return self._create_connection()
            logger.error(f"PostgreSQL 连接失败: {e}")
            raise
    
    def connect(self) -> bool:
        """建立数据库连接（兼容抽象接口）"""
        try:
            self.get_connection()
            return True
        except:
            return False
    
    def _create_database(self) -> bool:
        """创建数据库"""
        try:
            admin_conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database="postgres",
                user=self.user,
                password=self.password
            )
            admin_conn.autocommit = True
            cursor = admin_conn.cursor()
            cursor.execute(f"CREATE DATABASE {self.db_name}")
            cursor.close()
            admin_conn.close()
            logger.info(f"数据库 {self.db_name} 创建成功")
            return True
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"数据库 {self.db_name} 已存在")
                return True
            logger.error(f"创建数据库失败: {e}")
            return False
    
    def close(self):
        """关闭当前线程的数据库连接"""
        conn = getattr(self._local, 'conn', None)
        if conn:
            try:
                conn.close()
            except:
                pass
            self._local.conn = None
    
    def close_all(self):
        """关闭所有线程连接（仅用于测试清理）"""
        self._local.conn = None
        self._connected = False
    
    def get_connection(self):
        """获取数据库连接对象（线程安全）"""
        conn = getattr(self._local, 'conn', None)
        if conn and not conn.closed:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return conn
            except:
                pass
        conn = self._create_connection()
        self._local.conn = conn
        if not self._connected:
            self._connected = True
            logger.info(f"PostgreSQL 连接成功: {self.host}:{self.port}/{self.db_name}")
        return conn
    
    def execute(self, sql: str, params: Tuple = None) -> Any:
        """执行 SQL 语句"""
        conn = self.get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor
    
    def executemany(self, sql: str, params_list: List[Tuple]) -> Any:
        """批量执行 SQL 语句"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(sql, params_list)
        return cursor
    
    def fetchone(self, sql: str, params: Tuple = None) -> Optional[Tuple]:
        """执行查询并返回一行"""
        conn = self.get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchone()
    
    def fetchall(self, sql: str, params: Tuple = None) -> List[Tuple]:
        """执行查询并返回所有行"""
        conn = self.get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    
    def commit(self):
        """提交事务（兼容旧代码，当前使用 autocommit）"""
        pass
    
    def rollback(self):
        """回滚事务（兼容旧代码）"""
        pass
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=%s"
        result = self.fetchone(sql, (table_name,))
        return result is not None
    
    def upsert(self, table_name: str, columns: List[str], values: Tuple, pk_column: str) -> None:
        """
        执行 UPSERT 操作
        
        PostgreSQL 标准 UPSERT 语法：
        INSERT INTO table (cols) VALUES (vals) ON CONFLICT (pk) DO UPDATE SET col=EXCLUDED.col
        
        Args:
            table_name: 表名
            columns: 列名列表
            values: 值元组
            pk_column: 主键列名（用于 ON CONFLICT）
        """
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(values))
        
        update_cols = [f"{col}=EXCLUDED.{col}" for col in columns if col != pk_column]
        update_set = ", ".join(update_cols)
        
        if update_set:
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT ({pk_column}) DO UPDATE SET {update_set}"
        else:
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT ({pk_column}) DO NOTHING"
        
        self.execute(sql, values)
