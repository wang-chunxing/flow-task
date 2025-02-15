sequenceDiagram
    participant Scheduler
    participant Controller
    participant Translator
    participant Temporal
    participant DB

    Scheduler->>DB: 获取待调度任务
    Scheduler->>Controller: 申请执行槽位
    Controller-->>Scheduler: 返回槽位状态
    Scheduler->>DB: 获取工作流定义
    Scheduler->>Translator: 转换工作流定义
    Translator-->>Scheduler: 返回动态工作流类
    Scheduler->>Temporal: 注册工作流
    Scheduler->>Temporal: 启动工作流实例
    Temporal-->>Scheduler: 返回执行ID
    Scheduler->>DB: 更新任务状态



sequenceDiagram
    participant Scheduler
    participant Controller
    participant Queue
    participant Temporal
    
    Scheduler->>Controller: acquire_slot(task1)
    Controller->>Queue: 检查容量
    alt 有可用槽位
        Controller-->>Scheduler: 立即分配
        Scheduler->>Temporal: 提交任务
    else 无可用槽位
        Controller->>Queue: 加入等待队列
        loop 定期检查
            Controller->>Controller: 检查槽位释放
            Controller-->>Scheduler: 通知可重试
        end
        Scheduler->>Temporal: 延迟提交任务
    end



+----------------+     +----------------+     +-----------------+
|   TaskEngine   |     |   Persistence  |     |    Monitoring   |
|----------------|     |----------------|     |-----------------|
| - Submit Task  |---->| TaskStorage   |<---->| EventDispatcher |
| - Retry Logic  |     | WorkflowStorage|     +-----------------+
+----------------+     +----------------+     
       |                        |
       |                        v
       |              +-----------------+     +---------------+
       +------------->| TaskScheduler    |---->| Temporal      |
                       |-----------------|     |---------------|
                       | - DynamicScheduler |  | Workflow Engine|
                       | - ReliableScheduler | +---------------+
                       +-----------------+
                            ^        ^
                            |        |
                +-----------+        +-----------+
                |                                |
        +----------------+                +----------------+
        | WorkflowTranslator|                | ConcurrencyCtrl|
        |-----------------|                |----------------|
        | Code Generation  |                | Slot Management|
        +-----------------+                +----------------+



sequenceDiagram
    participant Client
    participant TaskEngine
    participant TaskStorage
    participant Scheduler
    participant WorkflowTranslator
    participant Temporal

    Client->>TaskEngine: submit_task(task)
    TaskEngine->>TaskStorage: save(task)
    TaskStorage-->>TaskEngine: saved_task
    TaskEngine->>Scheduler: add_job()
    
    Scheduler->>WorkflowTranslator: translate(workflow_def)
    WorkflowTranslator-->>Scheduler: workflow_class
    Scheduler->>Temporal: register_workflow()
    Scheduler->>Temporal: start_workflow()
    Temporal-->>Scheduler: execution_id
    Scheduler-->>TaskEngine: success
    TaskEngine-->>Client: task_created