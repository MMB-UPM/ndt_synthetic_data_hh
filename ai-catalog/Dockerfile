FROM elasticsearch:8.13.4

ENV discovery.type=single-node
ENV xpack.security.transport.ssl.enabled=false
ENV xpack.security.http.ssl.enabled=false
ENV xpack.security.enabled=false
ENV http.max_content_length=1gb
ENV cluster.routing.allocation.disk.watermark.high.max_headroom=100gb
ENV cluster.routing.allocation.disk.watermark.low.max_headroom=100gb

EXPOSE 9200
