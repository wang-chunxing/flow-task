.PHONY: install test start install-poetry install-pip generate-requirements sync-dependencies

# 使用 Poetry 安装依赖
install-poetry:
	poetry install

# 使用 Pip 安装依赖
install-pip:
	pip install -r requirements.txt

# 自动生成 requirements.txt 文件
generate-requirements:
	pipreqs . --force

# 同步所有依赖文件 (requirements.txt, pyproject.toml, setup.cfg)
sync-dependencies:
	@echo "Generating requirements.txt..."
	pipreqs . --force
	@echo "Updating pyproject.toml..."
	poetry export -f requirements.txt --without-hashes > requirements_pyproject.txt && \
	sed -i '' '/^$/d' requirements_pyproject.txt && \
	poetry add $(cat requirements_pyproject.txt | cut -d '=' -f 1)
	rm requirements_pyproject.txt
	@echo "Updating setup.cfg..."
	pip freeze > requirements_freeze.txt && \
	sed -i '' '1i\install_requires =\' requirements_freeze.txt && \
	sed -i '' 's/^/    /' requirements_freeze.txt && \
	sed -i '' '$$a\\' requirements_freeze.txt && \
	sed -i '' '/\[options\]/r requirements_freeze.txt' setup.cfg && \
	rm requirements_freeze.txt

# 运行测试
test:
	python -m unittest discover tests

# 启动应用
start:
	python -m app

# 构建 Docker 镜像
build-docker:
	docker build -t flow-task .

# 运行 Docker 容器
run-docker:
	docker run -p 8080:8080 flow-task

# 清理生成的文件
clean:
	find . -name "*.pyc" -exec rm -f {} +
	find . -name "*.pyo" -exec rm -f {} +
	find . -name "*~" -exec rm -f {} +

up:
	docker compose up --build -d

down:
	docker compose down

WITH_GROUP ?= ""

install-lint:
	poetry install --only ${WITH_GROUP}

lint: export WITH_GROUP = lint
lint: install-lint
	poetry run ruff check --config .ruff.toml --fix --unsafe-fixes
	poetry run importanize --config=importanize.json
	poetry run mypy --config-file=.mypy.ini .

format:
	ruff format --check