from twisted.internet import defer
import uuid
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

    def send_request(self, request):
        ''' Send a request
        
            Params
            ======
            request : dictionary
                Standard request message that contains a path, command, and
                args entries.

            Returns
            =======
            reply_received : defer.Deferred
                Deferred object that is fired when the reply to the request
                has been received.
        '''
        got_remote_service = self._get_remote_service(request)
        got_remote_service.addCallback(self._send_request, request, reply_received)
        # Create a new Deferred to indicate when the reply to the request has
        # been received
        reply_received = defer.Deferred()

        return reply_received

    def _get_remote_service(request):
        path = request['path']
        
        # First check the cache
        if path in self._remote_services:
            return defer.succeed(self._remote_services[path]['REQ'])
        got_remote_service = defer.Deferred()
        
        return got_remote_service

    def _send_request(self, request, reply_recieved):
        msg_id = uuid.uuid4().hex
        #TODO Finish
        raise NotImplementedError()

    def _socket(self, socketType, handler):
        socket = self._context.socket(socketType)
        self._sockets[socket] = handler
        self._poll.register(socket, zmq.POLLIN)
        
        return socket

    def _poll_once(self, timeout):
        # Poll the sockets using the timeout (milliseconds)
        sockets = dict(self._poll.poll(timeout))
        # Iterate through the returned values
        for socket, state in sockets:
            # Execute the handler 
            if state == zmq.POLLIN:
                self._sockets[socket](socket)

        self._task_schedule.execute()
