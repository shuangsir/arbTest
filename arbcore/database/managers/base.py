# arbcore/database/managers/base.py
# Manager 基类 - 使用数据库适配器

import threading
import logging
from typing import Optional

from ..adapters.base import IDatabaseAdapter

logger = logging.getLogger(__name__)


class BaseManager:
    """
    Manager 基类
    使用数据库适配器而不是直接连接 sqlite3
    """
    
    def __init__(self, adapter: IDatabaseAdapter, lock=None):
        """
        初始化 Manager
        
        Args:
            adapter: 数据库适配器实例
            lock: 线程锁（可选）
        """
        self.adapter = adapter
        self.lock = lock or threading.Lock()
    
    def _get_conn(self):
        """
        获取数据库连接（兼容旧代码）
        
        Returns:
            数据库连接对象
        """
        return self.adapter.get_connection()