from app.models.models import Workflow


# 后面可以实现一个本地执行器，用于本地调试
class WorkflowExecutor:
    def execute(self, workflow: Workflow, context: dict):
        pass
