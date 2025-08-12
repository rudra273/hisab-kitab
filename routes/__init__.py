from .user_routes import user_router
from .sms_transaction_routes import sms_transaction_router
from .system_routes import system_router
from .dashboard_routes import dash_router
from routes.chat_routes import chat_router


from fastapi import APIRouter

router = APIRouter()

router.include_router(user_router)
router.include_router(sms_transaction_router)
router.include_router(system_router)
router.include_router(dash_router)
router.include_router(chat_router)

