# 使用官方 Python 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制项目文件到容器中
COPY . .

# 安装依赖
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 暴露应用程序端口（如果适用）
EXPOSE 8080

# 设置默认命令以启动应用程序
CMD ["python", "-m", "app"]
