import importlib
import logging
import time
import types
import uuid
from typing import Any, Callable, Dict

import requests
from requests.exceptions import Timeout as RequestTimeout

from app.models.models import Operator
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
            id=model.id,
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


class FunctionOperator(Operator):
    def __init__(self, spec: dict, name: str):
        super().__init__(
            id=uuid.uuid4(),
            name=name,
            operator_type="function",
            spec=spec,
            version=1
        )
        self._func = None

    @property
    def func(self):
        if not self._func:
            module = importlib.import_module(self.spec["module"])
            self._func = getattr(module, self.spec["func_name"])
        return self._func

    def execute(self, *args, **kwargs):
        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Function operator failed: {e}", exc_info=True)
            raise


class APIOperator(Operator):
    def __init__(self, spec: dict, name: str):
        super().__init__(
            id=uuid.uuid4(),
            name=name,
            operator_type="async_api",
            spec=spec,
            version=1
        )
        self.timeout = self.spec.get("timeout", 10)
        self.run_config = spec.get("run_config", {})
        self.sync_config = spec.get("sync_config", {})
        self.total_timeout = spec.get("timeout", 300)
        self.poll_interval = spec.get("poll_interval", 5)

    def execute(self, data=None, params=None, headers=None):
        """执行API调用并根据配置处理异步轮询"""
        # 1. 执行主请求

        main_response = self.execute_main_request(data, params, headers)

        # 2. 处理异步等待逻辑
        if self.requires_async_polling():
            task_id = self.extract_task_id(main_response)
            return self.poll_async_task(task_id)

        return self.parse_response(main_response)

    def execute_main_request(self, data, params, headers):
        """执行主API请求"""
        try:
            return requests.request(
                method=self.run_config.get("method", "POST"),
                url=self.run_config["url"],
                headers=self.merge_headers(headers),
                params=self.merge_params(params),
                json=data,
                auth=self.run_config.get("auth"),
                timeout=self.run_config.get("request_timeout", 10)
            )
        except Exception as e:
            logger.error(f"Main request failed: {str(e)}")
            raise RuntimeError(f"API operator failed initial request: {str(e)}") from e

    def requires_async_polling(self):
        """检查是否需要异步轮询"""
        return bool(self.spec.get("sync_policy") == "true")

    def extract_task_id(self, response):
        """从主响应中提取任务ID"""
        try:
            return self.nested_get(
                response.json(),
                self.run_config.get("task_id_path", "id").split(".")
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to extract task ID: {str(e)}")
            raise RuntimeError("Failed to extract async task ID") from e

    def poll_async_task(self, task_id):
        """轮询异步任务状态"""
        start_time = time.time()
        while time.time() - start_time < self.total_timeout:
            try:
                response = self.execute_sync_request(task_id)
                status = self.extract_status(response)

                if status in self.sync_config.get("success_status", []):
                    return self.parse_response(response)
                if status in self.sync_config.get("failure_status", []):
                    raise RuntimeError(f"Async task failed with status: {status}")

                time.sleep(self.poll_interval)
            except RequestTimeout as e:
                logger.warning(f"Polling timeout: {str(e)}")
                continue

        raise TimeoutError(f"Async task timed out after {self.total_timeout}s")

    def execute_sync_request(self, task_id):
        """执行状态检查请求"""
        try:
            return requests.request(
                method=self.sync_config.get("method", "GET"),
                url=self.format_sync_url(task_id),
                headers=self.sync_config.get("headers", {}),
                params=self.sync_config.get("params", {}),
                auth=self.sync_config.get("auth"),
                timeout=self.sync_config.get("request_timeout", self.poll_interval)
            )
        except Exception as e:
            logger.error(f"Sync request failed: {str(e)}")
            raise RuntimeError("Failed to poll async task status") from e

    def format_sync_url(self, task_id):
        """格式化包含任务ID的URL"""
        return self.sync_config["url"].replace("{task_id}", str(task_id))

    def extract_status(self, response):
        """从响应中提取状态"""
        try:
            return self.nested_get(
                response.json(),
                self.sync_config.get("status_path", "status").split(".")
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to extract status: {str(e)}")
            raise RuntimeError("Invalid status response format") from e

    @staticmethod
    def nested_get(data, path):
        """从嵌套字典中获取值"""
        for key in path:
            data = data[key]
        return data

    def merge_headers(self, headers):
        return {**self.run_config.get("headers", {}), **(headers or {})}

    def merge_params(self, params):
        return {**self.run_config.get("params", {}), **(params or {})}

    def parse_response(self, response):
        response.raise_for_status()
        return response.json()

