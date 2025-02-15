# import unittest
#
# from app.container import CoreContainer, BusinessContainer, ApplicationContainer
# from app.core.models import TaskQueue
# from app.builders import TaskBuilder, WorkflowBuilder
# from app.persistence.abstract import Base
#
#
# def validate_order(order_data: dict):
#     """订单验证函数"""
#     if not order_data.get("items"):
#         raise ValueError("Invalid order: no items")
#     if not order_data.get("user_id"):
#         raise ValueError("Invalid user ID")
#     print(f"Validated order for user {order_data['user_id']}")
#     return {"status": "validated"}
#
#
# def send_confirmation_email(context: dict):
#     """邮件发送函数"""
#     email_content = f"Order {context['order_id']} confirmed. Total: {context['total_amount']}"
#     print(f"Sending email: {email_content}")
#     return {"email_status": "sent"}
#
#
# def log_error(context: dict):
#     """错误日志记录函数"""
#     error = context.get("error")
#     print(f"Logging error: {error}")
#     return {"logged": True}
#
#
# def notify_team(context: dict):
#     """通知团队函数"""
#     print(f"Alerting team about failed order: {context.get('order_id')}")
#     return {"notified": True}
#
#
# class TestTaskCreate(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         # 初始化容器
#         cls.core = CoreContainer(config={
#             "database_connection": "mysql+pymysql://root:password@localhost/flow_task"
#         })
#         cls.business = BusinessContainer(core=cls.core)
#
#         # 创建数据库表
#         Base.metadata.create_all(cls.core.db_engine())
#
#         cls.app = ApplicationContainer(business=cls.business, core=cls.core)
#
#     def build_order_workflow(self, builder: WorkflowBuilder) -> WorkflowBuilder:
#         """创建分层订单处理工作流"""
#         return (
#             builder
#             # 验证阶段
#             .stage("validation")
#                 .add_operator(
#                     self.business.operator_registry().get_operator("order_validator"),
#                     depends_on=[]
#                 )
#             # 库存阶段
#             .stage("inventory")
#                 .add_operator(
#                     self.business.operator_registry().get_operator("inventory_checker"),
#                     depends_on=["order_validator"]  # 依赖验证阶段
#                 )
#             # 支付阶段
#             .stage("payment")
#                 .add_operator(
#                     self.business.operator_registry().get_operator("payment_processor"),
#                     depends_on=["inventory_checker"]  # 依赖库存阶段
#                 )
#             # 通知阶段
#             .stage("notification")
#                 .add_operator(
#                     self.business.operator_registry().get_operator("email_sender"),
#                     depends_on=["payment_processor"]  # 依赖支付阶段
#                 )
#         )
#
#
#     def test_task_creation(self):
#         self.business.operator_registry().register_function(
#             "order_validator", validate_order)
#         self.business.operator_registry().register_function(
#             "email_sender", send_confirmation_email)
#         self.business.operator_registry().register_function(
#             "error_logger", log_error)
#         self.business.operator_registry().register_function(
#             "team_notifier", notify_team)
#
#         # ================== 注册API算子 ==================
#         # 库存检查API
#         self.business.operator_registry().register_api(
#             "inventory_checker",
#             url="http://inventory-service/api/check",
#             method="POST",
#             headers={"Content-Type": "application/json"},
#             timeout=15
#         )
#
#         # 支付处理API
#         self.business.operator_registry().register_api(
#             "payment_processor",
#             url="http://payment-gateway/api/charge",
#             method="POST",
#             headers={"Authorization": "Bearer API_KEY"},
#             timeout=30
#         )
#
#         # 构建完整任务
#         order_task = (
#             TaskBuilder("process_order")
#             .set_scheduler("interval", minutes=5)  # 每5分钟执行一次
#             .set_queue("")
#             .build_workflow(self.build_order_workflow)
#             .on_success(
#                 self.business.operator_registry().get_operator("order_validator")
#             )
#             .on_failure(
#                 self.business.operator_registry().get_operator("error_logger"),
#                 self.business.operator_registry().get_operator("team_notifier")
#             )
#             .build()
#         )
#
#         # 验证工作流结构
#         workflow = order_task.workflow
#
#         # 打印提交的任务是否成功
#         result=self.app.task_engine().submit_task(order_task)
#         print(f"任务提交结果：{result}")
#
#
#         # 1. 验证阶段数量
#         assert len(workflow.stages) == 4, "Should have 4 stages"
#
#         # 2. 验证执行层次
#         execution_layers = workflow.execution_layers
#         assert len(execution_layers) == 4, "Should have 4 execution layers"
#
#         # 3. 验证依赖顺序
#         layer_names = [[op.name for op in layer] for layer in execution_layers]
#         assert layer_names == [
#             ["order_validator"],
#             ["inventory_checker"],
#             ["payment_processor"],
#             ["email_sender"]
#         ], "Execution layers are not in correct order"
#
#         # 4. 验证错误处理
#         assert len(order_task.failure_handlers) == 2, "Should have 2 failure handlers"
#
#         # 5. 验证调度配置
#         assert order_task.scheduler_config.scheduler_type == "interval"
#         assert order_task.scheduler_config.config["minutes"] == 5
#
#         print("所有工作流验证通过！")
#         print("生成的任务结构：")
#         print(f"任务名称：{order_task.name}")
#         print(f"任务队列：{order_task.queue.value}")
#         print(f"包含阶段：{[stage.name for stage in workflow.stages]}")
#         print(f"执行层次：{layer_names}")
#
