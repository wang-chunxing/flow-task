import json
import sys
import unittest
from datetime import datetime, timedelta
from types import ModuleType
from unittest import mock

import numpy as np

from app.container import BusinessContainer, CoreContainer
from app.persistence.adapters.operator import Base, OperatorModel


def _create_sample_function():
    """动态创建可导入的测试函数"""
    # 创建虚拟模块
    module_name = "test_operators_module"
    if module_name not in sys.modules:
        sys.modules[module_name] = ModuleType(module_name)

    module = sys.modules[module_name]

    # 定义测试函数
    def test_func(data):
        """测试处理函数"""
        return data * 2

    # 将函数附加到模块
    test_func.__module__ = module_name
    test_func.__name__ = "test_func"
    setattr(module, "test_func", test_func)

    return test_func


def _create_complex_function():
    """创建包含多种功能的复杂测试函数模块"""
    module_name = "complex_operators_module"
    if module_name not in sys.modules:
        sys.modules[module_name] = ModuleType(module_name)

    module = sys.modules[module_name]

    # 复杂函数1：带类型验证和异常处理的处理函数
    def data_processor(input_data, multiplier=2, log_errors=True):
        """
        数据处理函数：
        - 支持多种输入类型（dict, list, int）
        - 异常处理机制
        - 默认参数使用
        """
        try:
            if isinstance(input_data, dict):
                return {k: v * multiplier for k, v in input_data.items()}
            elif isinstance(input_data, list):
                return [x * multiplier for x in input_data]
            elif isinstance(input_data, (int, float)):
                return input_data * multiplier
            else:
                raise ValueError("Unsupported input type")
        except Exception as e:
            if log_errors:
                print(f"Error processing data: {str(e)}")
            raise

    # 复杂函数2：包含第三方库使用的函数
    def calculate_bmi(weight_kg, height_cm):
        """BMI计算函数"""
        height_m = height_cm / 100
        return np.round(weight_kg / (height_m ** 2), 2)

    # 复杂函数3：时间序列处理函数
    def generate_time_series(start_date, days=7):
        """生成时间序列"""
        date_format = "%Y-%m-%d"
        start = datetime.strptime(start_date, date_format)
        return [{
            "date": (start + timedelta(days=i)).strftime(date_format),
            "value": i * 100
        } for i in range(days)]

    # 复杂函数4：带依赖注入的配置处理
    def configurable_processor(data, config_json):
        """可配置的数据处理器"""
        config = json.loads(config_json)
        if config.get("reverse") and isinstance(data, list):
            return data[::-1]
        return data

    # 将函数附加到模块
    for func in [data_processor, calculate_bmi, generate_time_series, configurable_processor]:
        func.__module__ = module_name
        setattr(module, func.__name__, func)

    return data_processor, calculate_bmi, generate_time_series, configurable_processor


class TestOperatorRegistry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 初始化容器
        cls.core = CoreContainer(config={
            "database_connection": "mysql+pymysql://root:password@localhost/flow_task"
        })
        cls.business = BusinessContainer(core=cls.core)

        # 创建数据库表
        Base.metadata.create_all(cls.core.db_engine())

    def setUp(self):
        # 每个测试前清理数据库
        session = self.core.db_session_factory()()
        session.query(OperatorModel).delete()
        session.commit()
        session.close()

        # 重新初始化注册表
        self.registry = self.business.operator_registry()
        self.registry._load_operators()

    def test_register_and_execute_function_operator(self):
        """测试函数算子全链路：注册-存储-加载-执行"""
        # 获取动态创建的测试函数
        test_func = _create_sample_function()

        # 注册函数算子
        self.registry.register_function("data_processor", test_func)

        # 验证数据库存储
        session = self.core.db_session_factory()()
        db_operator = session.query(OperatorModel).filter_by(name="data_processor").first()
        self.assertIsNotNone(db_operator)
        self.assertEqual(db_operator.operator_type, "function")

        # 验证内存加载
        self.assertIn("data_processor", self.registry.operators)

        # 执行算子
        operator = self.registry.get_operator("data_processor")
        result = operator.execute(100)
        print(result)
        self.assertEqual(result, 200)

    def test_complex_data_processing(self):
        data_processor, calculate_bmi, generate_series, config_processor = _create_complex_function()
        # 注册处理器
        self.registry.register_function("complex_processor", data_processor)

        operator = self.registry.get_operator("complex_processor")

        # 测试字典处理
        dict_result = operator.execute({"a": 1, "b": 2}, 3)
        self.assertEqual(dict_result, {"a": 3, "b": 6})

        # 测试列表处理
        list_result = operator.execute([1, 2, 3])
        self.assertEqual(list_result, [2, 4, 6])

        # 测试错误处理
        with self.assertRaises(ValueError):
            operator.execute("invalid input")

        # 第三方库使用验证
        self.registry.register_function("bmi_calculator", calculate_bmi)

        operator = self.registry.get_operator("bmi_calculator")
        result = operator.execute(70, 175)
        self.assertEqual(result, 22.86)

        # 时间序列生成测试

        self.registry.register_function("time_series", generate_series)

        operator = self.registry.get_operator("time_series")
        result = operator.execute("2024-01-01", 3)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["date"], "2024-01-01")

        # 配置驱动处理测试

        self.registry.register_function("config_processor", config_processor)

        operator = self.registry.get_operator("config_processor")

        # 正常处理
        data = [1, 2, 3]
        normal_result = operator.execute(data, '{"reverse": false}')
        self.assertEqual(normal_result, data)

        # 反转处理
        reversed_result = operator.execute(data, '{"reverse": true}')
        self.assertEqual(reversed_result, [3, 2, 1])

    def test_register_and_execute_api_operator(self):
        """测试API算子全链路：注册-存储-加载-执行（正常流程）"""
        # 注册API算子
        self.registry.register_api(
            name="weather_api",
            url="https://api.weatherapi.com/v1/current.json",
            method="GET",
            params={"key": "API_KEY"},
            headers={"Accept": "application/json"},
            timeout=15
        )

        # 验证数据库存储
        session = self.core.db_session_factory()()
        db_operator = session.query(OperatorModel).filter_by(name="weather_api").first()
        self.assertIsNotNone(db_operator)
        self.assertEqual(db_operator.operator_type, "api")
        self.assertEqual(db_operator.spec["url"], "https://api.weatherapi.com/v1/current.json")
        session.close()

        # 验证内存加载
        self.assertIn("weather_api", self.registry.operators)

        # 模拟成功响应
        mock_response = {
            "location": {"name": "Beijing"},
            "current": {"temp_c": 25.0}
        }
        with mock.patch('requests.request') as mock_request:
            # 配置模拟响应
            mock_response_obj = mock.Mock()
            mock_response_obj.status_code = 200
            mock_response_obj.json.return_value = mock_response
            mock_request.return_value = mock_response_obj

            # 执行算子
            operator = self.registry.get_operator("weather_api")
            result = operator.execute(params={"q": "Beijing"})

            # 验证请求参数
            mock_request.assert_called_once_with(
                method="GET",
                url="https://api.weatherapi.com/v1/current.json",
                json=None,
                params={"key": "API_KEY", "q": "Beijing"},
                headers={"Accept": "application/json"},
                auth=None,
                timeout=15
            )

            # 验证结果处理
            self.assertEqual(result, mock_response)

    def test_api_operator_parameter_handling(self):
        """测试API算子的参数传递逻辑"""
        # 注册不同参数类型的API
        self.registry.register_api(
            name="complex_api",
            url="https://api.example.com/complex",
            method="PUT",
            headers={"X-Custom-Header": "123"},
            auth=("user", "pass"),
            timeout=30
        )
        operator = self.registry.get_operator("complex_api")

        # 模拟请求
        with mock.patch('requests.request') as mock_request:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"status": "success"}
            mock_request.return_value = mock_response

            # 执行带复杂参数的请求
            operator.execute(
                data={"payload": "data"},
                params={"page": 2},
                headers={"X-Extra-Header": "extra"}  # 测试动态header合并
            )

            # 验证请求参数组合
            expected_headers = {
                "X-Custom-Header": "123",
                "X-Extra-Header": "extra"
            }
            mock_request.assert_called_once_with(
                method="PUT",
                url="https://api.example.com/complex",
                json={"payload": "data"},
                params={"page": 2},
                headers=expected_headers,
                auth=("user", "pass"),
                timeout=30
            )

# 运行测试


if __name__ == "__main__":
    unittest.main()
