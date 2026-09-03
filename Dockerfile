# Sequoia-X Web 控制台 Docker 镜像
#
# 构建：docker build -t sequoia-x .
# 运行：docker run -d --name sequoia-x -p 8000:8000 -v ${PWD}/data:/app/data sequoia-x
#       （数据目录挂载宿主机 data/，数据库在容器内持久化，重启不丢）

FROM python:3.12-slim

# 环境：强制无缓冲输出 + UTF-8，避免日志与中文乱码
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

WORKDIR /app

# 先装依赖（利用 Docker 层缓存：源码改动不重复下载依赖）
# 依赖清单与 pyproject.toml 保持一致；不执行 `pip install .` 因为项目未声明 build-system
RUN pip install --no-cache-dir \
    "akshare>=1.10" \
    "baostock>=0.9" \
    "pydantic-settings>=2.0" \
    "python-dotenv>=1.0" \
    "rich>=13.0" \
    "pandas>=2.0" \
    "requests>=2.31"

# 拷贝源码（data/ 等由 .dockerignore 排除，不进镜像）
COPY . .

# 数据库目录作为卷：挂载宿主机 data/ 即可持久化
VOLUME ["/app/data"]

# 容器内监听 0.0.0.0（宿主机通过 -p 访问）；端口可用 SEQUOIA_PORT 覆盖
ENV SEQUOIA_HOST=0.0.0.0 \
    SEQUOIA_PORT=7860

EXPOSE 7860

CMD ["python", "webconsole/app.py"]
