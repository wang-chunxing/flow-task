import importlib
import logging
import types
import uuid
from typing import Any, Callable, Dict

from app.models.models import Operator, FunctionOperator, APIOperator
from app.persistence.abstract import OperatorNotFoundError
from app.persistence.adapters.operator import OperatorStorage


logger = logging.getLogger(__name__)


class OperatorRegistry:
    def __init__(self, operator_repository: OperatorStorage):
        self.storage = operator_repository
        self.operators: Dict[str, Operator] = {}

    async def initialize(self):
        await self._init_registry()

    async def _init_registry(self):
        """初始化时加载所有算子"""
        try:
            operators = await self.storage.load_all()
            for op in operators:
                self.operators[op.name] = Operator(
                    id=op.id,
                    name=op.name,
                    operator_type=op.operator_type,
                    spec=op.spec,
                    version=op.version,
                )
        except Exception as e:
            logger.error(f"Failed to initialize operator registry: {str(e)}")
            raise

    async def register_function(self, name: str, func: Callable):
        """Register a function operator with validation"""
        if not isinstance(func, (types.FunctionType, types.MethodType)):
            raise ValueError("Only functions/methods can be registered")

        if not hasattr(func, '__module__') or not hasattr(func, '__name__'):
            raise ValueError("Cannot register non-module functions")

        try:
            module = importlib.import_module(func.__module__)
            imported_func = getattr(module, func.__name__)
        except ImportError as e:
            raise ValueError(f"Module {func.__module__} not found: {e}")
        except AttributeError as e:
            raise ValueError(f"Function {func.__name__} not found: {e}")

        if imported_func != func:
            raise ValueError("Imported function mismatch")

        spec = {
            "module": func.__module__,
            "func_name": func.__name__,
        }
        await self._register(name, "function", spec)

    async def register_api(
            self,
            name: str,
            # 执行触发配置
            run_config: Dict[str, Any],
            # 状态检查配置
            sync_config: Dict[str, Any],
            # 公共配置
            timeout: int = 300,
            poll_interval: int = 5,
            sync_policy: str = "true",
    ):
        """注册异步API算子"""
        spec = {
            "type": "async_api",
            "run_config": run_config,
            "sync_config": sync_config,
            "timeout": timeout,
            "poll_interval": poll_interval,
            "sync_policy": sync_policy
        }
        await self._register(name, "async_api", spec)

    async def _register(self, name: str, operator_type: str, spec: dict):
        # 检查是否存在现有算子
        print(f"Registering operator: {name}")  # 调试日志

        try:
            # 尝试获取最新版本
            existing = await self.storage.load(name)
            new_version = existing.version + 1
        except OperatorNotFoundError:
            # 首次注册的情况
            new_version = 1

        """统一注册方法"""
        # 直接尝试持久化，依赖数据库的唯一性约束
        model = Operator(
            id=uuid.uuid4(),
            name=name,
            operator_type=operator_type,
            spec=spec,
            version=new_version
        )
        try:
            await self.storage.save(model)
        except ValueError as e:
            raise ValueError(f"Registration failed: {str(e)}") from e

        # 更新内存注册表
        self.operators[name] = Operator(
            name=name,
            operator_type=operator_type,
            spec=spec,
            version=model.version
        )

    def get_operator(self, name: str):
        if spec := self.operators.get(name):
            return self._create_operator(name, spec)
        raise KeyError(f"Operator '{name}' not found")

    def _create_operator(self, name: str, spec: Operator):
        if spec.operator_type == "function":
            return FunctionOperator(spec.spec, name)
        else:
            return APIOperator(spec.spec, name)



