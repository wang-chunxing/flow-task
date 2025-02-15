-- 创建业务数据库（根据你的需求）
CREATE DATABASE IF NOT EXISTS flow_task;
GRANT ALL PRIVILEGES ON flow_task.* TO 'user'@'%';

-- 创建 Temporal 核心数据库
CREATE DATABASE IF NOT EXISTS temporal;
GRANT ALL PRIVILEGES ON temporal.* TO 'user'@'%';

-- 创建 Temporal 可见性数据库（关键修复点）
CREATE DATABASE IF NOT EXISTS temporal_visibility;
GRANT ALL PRIVILEGES ON temporal_visibility.* TO 'user'@'%';

-- 刷新权限
FLUSH PRIVILEGES;