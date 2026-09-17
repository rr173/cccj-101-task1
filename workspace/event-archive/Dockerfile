# 仅依赖 Python 标准库；slim 镜像即可
FROM python:3.11-slim

WORKDIR /srv
COPY app /srv/app

# 非 root 运行；数据目录由命名卷挂入
RUN useradd --create-home --uid 10001 archiver \
    && mkdir -p /data \
    && chown -R archiver:archiver /data /srv
USER archiver

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).status==200 else 1)"

ENTRYPOINT ["python", "-m", "app.main"]
