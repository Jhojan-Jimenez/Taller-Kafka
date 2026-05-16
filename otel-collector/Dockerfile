# Imagen custom del OTel Collector con la config embebida.
# Necesaria para Container Apps, donde no hay ConfigMaps de K8s.
# La clave NEW_RELIC_LICENSE_KEY se pasa como variable de entorno en runtime.
FROM otel/opentelemetry-collector-contrib:0.111.0
COPY config.yaml /etc/otelcol-contrib/config.yaml
