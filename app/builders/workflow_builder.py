from typing import List, Optional

from app.models.models import DAG, Operator, Stage, Workflow


class WorkflowBuilder:
    """工作流构建器，支持DSL语法"""
    def __init__(self):
        self.name: str = ""
        self.stages: dict[str, Stage] = {}
        self.global_dag = DAG()
        self._current_stage: Optional[Stage] = None

    def __call__(self, name: str) -> 'WorkflowBuilder':
        """创建工作流"""
        self.name = name
        return self

    def stage(self, name: str, description: str | None = "") -> 'WorkflowBuilder':
        """开始定义新阶段"""
        if name in self.stages:
            raise ValueError(f"Stage {name} already exists")
        self._current_stage = Stage(name=name, description=description)
        self.stages[name] = self._current_stage
        return self

    def add_operator(self, operator: 'Operator', depends_on: List[str] | None = None):
        """向当前阶段添加算子"""
        if not self._current_stage:
            raise RuntimeError("No active stage, create stage first")
        self._current_stage.add_operator(operator, depends_on)

        # 更新全局DAG
        self.global_dag.add_node(operator.name)
        for dep in (depends_on or []):
            self.global_dag.add_node(dep)
            self.global_dag.add_edge(dep, operator.name)
        return self

    def build(self) -> Workflow:
        """生成分层执行计划"""
        # 收集所有算子
        all_operators = {}
        for stage in self.stages.values():
            for op_name, operator in stage.operators.items():
                if op_name in all_operators:
                    raise ValueError(f"Duplicate operator name: {op_name}")
                all_operators[op_name] = operator

        # 验证依赖
        for stage in self.stages.values():
            for op_name, deps in stage.dependencies.items():
                for dep in deps:
                    if dep not in all_operators:
                        raise ValueError(f"Dependency {dep} not found")

        # 生成分层执行计划
        try:
            layer_names = self.global_dag.layered_topological_sort()
        except ValueError as e:
            raise RuntimeError(f"Workflow validation failed: {str(e)}") from e

        # 转换为算子对象的层次结构
        execution_layers = [
            [all_operators[name] for name in layer]
            for layer in layer_names
        ]

        return Workflow(
            name=self.name,
            stages=list(self.stages.values()),
            execution_layers=execution_layers
        )

    @staticmethod
    def _validate_workflow(self, workflow: Workflow):
        """工作流数据校验"""
        # 检查所有依赖算子存在
        all_operator_names = {op.name for layer in workflow.execution_layers for op in layer}
        for stage in workflow.stages:
            for deps in stage.dependencies.values():
                for dep in deps:
                    if dep not in all_operator_names:
                        raise ValueError(f"依赖算子 {dep} 不存在")

        # 检查执行计划完整性
        execution_operators = {op.name for layer in workflow.execution_layers for op in layer}
        for stage in workflow.stages:
            stage_operators = set(stage.operators.keys())
            if not stage_operators.issubset(execution_operators):
                missing = stage_operators - execution_operators
                raise ValueError(f"阶段 {stage.name} 包含未在执行计划中的算子: {missing}")

