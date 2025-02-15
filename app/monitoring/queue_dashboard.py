# from collections import defaultdict
#
# from app.scheduling.concurrency import ConcurrencyController
#
#
# class QueueDashboard:
#     """实时队列监控看板"""
#
#     def __init__(self, controller: ConcurrencyController):
#         self.controller = controller
#         self.metrics = defaultdict(dict)
#
#     async def refresh_metrics(self):
#         """刷新所有队列指标"""
#         queues = await self.controller.storage.list_queues()
#         for q in queues:
#             self.metrics[q.name] = {
#                 'capacity': q.max_concurrent,
#                 'active': q.current_concurrent,
#                 'waiting': len(self.controller.pending_tasks[q.name]),
#                 'throughput': await self._calculate_throughput(q.name)
#             }
#
#     async def auto_adjust_queues(self):
#         """根据负载自动调整队列容量"""
#         for q_name, metrics in self.metrics.items():
#             if metrics['active'] / metrics['capacity'] > 0.8:
#                 await self._scale_up_queue(q_name)
#             elif metrics['active'] / metrics['capacity'] < 0.2:
#                 await self._scale_down_queue(q_name)
#
#     async def _scale_up_queue(self, queue_name: str):
#         """扩容队列"""
#         new_cap = min(
#             self.metrics[queue_name]['capacity'] * 2,
#             MAX_QUEUE_CAPACITY
#         )
#         await self.controller.storage.update_queue_capacity(
#             queue_name, new_cap
#         )
#
#     async def _scale_down_queue(self, queue_name: str):
#         """缩容队列"""
#         new_cap = max(
#             self.metrics[queue_name]['capacity'] // 2,
#             MIN_QUEUE_CAPACITY
#         )
#         await self.controller.storage.update_queue_capacity(
#             queue_name, new_cap
#         )
