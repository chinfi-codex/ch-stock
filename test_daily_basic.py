#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试 daily_basic 存储功能
"""

import sys
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    from infra.database import init_database, get_db_stats
    from tools.daily_basic_storage import (
        get_daily_basic_smart,
        save_daily_basic_async,
        check_data_existence,
        get_last_sync_date
    )
    from services.daily_basic_sync import (
        sync_single_date,
        get_sync_status,
        sync_recent_days
    )
    
    print("Success: All modules imported")
    print("=" * 60)
    
    # 1. 初始化数据库
    print("\n1. Initializing database...")
    init_database()
    print("Success: Database initialized")
    
    # 2. 查看数据库状态
    print("\n2. Database status:")
    stats = get_db_stats()
    for k, v in stats.items():
        print(f"   {k}: {v}")
    
    # 3. 测试获取数据（智能查询）
    print("\n3. Testing smart data fetch...")
    today = datetime.now().strftime("%Y%m%d")
    print(f"   Date: {today}")
    
    df = get_daily_basic_smart(trade_date=today, use_cache=True)
    
    if df is not None and not df.empty:
        print(f"   Success: Got {len(df)} records")
        print(f"   Columns: {list(df.columns)}")
        print(f"   First 3 rows:")
        print(df.head(3))
    else:
        print(f"   Warning: No data (might be non-trading day or invalid token)")
    
    # 4. 检查同步状态
    print("\n4. Sync status:")
    sync_status = get_sync_status()
    for k, v in sync_status.items():
        print(f"   {k}: {v}")
    
    print("\n" + "=" * 60)
    print("Success: Test completed!")
    print("\nNext steps:")
    print("  1. Backfill history: python services/daily_basic_sync.py sync_all 20260101")
    print("  2. Sync recent 7 days: python services/daily_basic_sync.py sync_recent 7")
    print("  3. Check status: python services/daily_basic_sync.py status")
    
except Exception as e:
    print(f"Error: Test failed - {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
