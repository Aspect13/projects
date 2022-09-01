import redis
import json

from tools import rpc_tools
from pylon.core.tools import web, log

from tools import constants


class RPC:
    @web.rpc('update_rabbit_queues', 'update_rabbit_queues')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def update_rabbit_queues(self, vhost, queues):
        _rc = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=4,
                          password=constants.REDIS_PASSWORD, username=constants.REDIS_USER)
        _rc.set(name=vhost, value=queues)
        return f"Project queues updated"

    @web.rpc('register_rabbit_queue', 'register_rabbit_queue')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def register_rabbit_queue(self, vhost, queue_name):
        _rc = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=4,
                          password=constants.REDIS_PASSWORD, username=constants.REDIS_USER)
        queues = _rc.get(name=vhost)
        queues = json.loads(queues) if queues else []
        if queue_name not in queues:
            queues.append(queue_name)
            _rc.set(name=vhost, value=json.dumps(queues))
            return f"Queue with name {queue_name} registered"
        return f"Queue with name {queue_name} already exist"

    @web.rpc('get_rabbit_queues', 'get_rabbit_queues')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_rabbit_queues(self, vhost):
        _rc = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=4,
                          password=constants.REDIS_PASSWORD, username=constants.REDIS_USER)
        try:
            return json.loads(_rc.get(name=vhost))
        except TypeError:
            return []
