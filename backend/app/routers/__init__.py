"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import delivery, resource

# delivery 使用 /api/delivery/... 固定路径，必须排在 /api/{module} 之前。
ROUTERS = [delivery, resource]
