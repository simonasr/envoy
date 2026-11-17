Added ``LOCAL_ZONE_AFFINITY_ANY`` to the :ref:`read policy <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.ConnPoolSettings.read_policy>`
options to select randomly among healthy local primary and replica nodes, falling back to healthy
shard members globally and then all shard members when none are healthy.
