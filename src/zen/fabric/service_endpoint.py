import zmq

from zen.fabric.task_schedule import TaskSchedule

class ServiceEndpoint(object):
    ''' Service End-point
    
        An end-point for services and service clients.
    '''

    def __init__(self):
        self._context = zmq.Context()
        self._poll = zmq.Poller()
        self._task_schedule = TaskSchedule()

        # Dictionary of sockets and the handler
        self._sockets = {}

    def _socket(self, socketType, handler):
        socket = self._context.socket(socketType)
        self._sockets[socket] = handler
        self._poll.register(socket, zmq.POLLIN)

    def _poll_once(self, timeout):
        # Poll the sockets using the timeout (milliseconds)
        sockets = dict(self._poll.poll(timeout))
        # Iterate through the returned values
        for socket, state in sockets:
            # Execute the handler 
            if state == zmq.POLLIN:
                self._sockets[socket]()

        self._task_schedule.execute()
