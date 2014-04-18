import json
import traceback
from twisted.internet import defer
import uuid
import zmq

from zen.fabric.task_schedule import TaskSchedule
from zen.fabric.service_registry.proxy import ServiceRegistryProxy
from zen.fabric.json_util import filter_json

class ServiceEndpoint(object):
    ''' Service End-point
    
    An Enterprise Service Bus end-point for services and service clients.
    '''
    def __init__(self, context=None):
        if context:
            self._context = context
        else:
            self._context = zmq.Context()
        self._poll = zmq.Poller()
        self._task_schedule = TaskSchedule()
        self._service_registry = None
        # Dictionary of sockets and the handler
        self._sockets = {}
        # Outstanding requests - msg_id : callback
        self._requests = {}

    def init(self, srap):
        ''' Initialize the service endpoint
        
        Params
        ------
        srap : string, optional
            String of address:port where the service registry is located
        '''
        if self._service_registry is not None:
            #TODO Log
            print('Error, already connected to service registry.')
            raise RuntimeError('Already connected to service registry')

        self._service_registry = ServiceRegistryProxy(self, srap)

    def run(self, pacing=1000):
        ''' Run the service container in the current thread.

        Params
        ------
        pacing : int
            timeout in milliseconds for each poll; this is the finest 
            granularity of timeouts and other timed events.
        '''
        # Indicate that the container is running
        self._is_running = True
        
        # Loop until something has indicated that the container is no longer 
        # running
        while self._is_running:
            try:
                self._poll_once(pacing)
            except KeyboardInterrupt as e:
                print('Got an exception:{0}'.format(traceback.format_exc()))
                print('Shutting down via keyboard interrupt...')
                self.shutdown()
            except:
                #TODO Handle errors instead of simply ignoring them.
                print('Got an exception:{0}'.format(traceback.format_exc()))
                continue

    def shutdown(self):
        self._is_running = False

    def _error(self, error):
        print('Error: {0}'.format(error))

    def send_request(self, request):
        ''' Send a request. This assumes the request has a path, and it uses
        the service registry to determine which socket can handle the request 
        based on the service registry.
        
        Params
        ------
        request : dictionary
            Standard request message that contains a path, command, and
            args entries.

        Returns
        -------
        reply_received : defer.Deferred
            Deferred object that is fired when the reply to the request
            has been received.
        '''
        if 'path' not in request:
            raise RuntimeError('Cannot send request without a path')
        
        # Create a new Deferred to indicate when the reply to the request has
        # been received
        reply_received = defer.Deferred()

        # The reponse will contain the message id, so associate the Deferred with 
        # this message id so it can be activated when the response is returned.
        msg_id = uuid.uuid4().hex
        self._requests[msg_id] = reply_received

        # Asyncronously get the remote socket
        got_remote_socket = self._service_registry.get_remote_socket(request)

        # After that's gotten, send the request to that socket
        got_remote_socket.addCallback(self.send_message_to_socket, request, msg_id)
        got_remote_socket.addErrback(self._error)

        return reply_received

    def connect_request(self, address):
        ''' Connect to the specified address for sending request messages.
        
        Params
        ------
        address : string
            address:port
        '''
        socket = self.socket(zmq.REQ, self._handle_response)
        connect_string = 'tcp://{0}'.format(address)
        print('ServiceEndpoint.connect_request: Connecting to {0}'.format(connect_string))
        socket.connect(connect_string)
        return socket

    def send_message_to_socket(self, socket, message, msg_id=None):
        ''' Send a request to the specified socket.  

        Params
        ------
        socket : zmq.Socket
            Socket through which the message will be sent
        message : string
            Message to be sent
        msg_id : string
            Unique message identifier
            
        Returns
        -------
        socket : zmq.Socket
            Returns the socket; used when chaining deferreds
        '''
        print('Sending to {0}'.format(socket))
        if msg_id is None:
            msg_id = uuid.uuid4().hex

        message_str = json.dumps(message)
        print('SND: {0}'.format(message_str))
        socket.send_multipart([msg_id, message_str])
        return socket

    def socket(self, socketType, handler):
        ''' Construct a server socket and register it for input polling as well
        as register a handler to call when the socket receives request messages.

        Params
        ------
        socketType : int
            zmq socket type (zmq.REP is the normal socket type)
        handler : function
            Function that will handle any inbound requests.  The function
            should take on parameter, which is the zmq socket on which
            the request is being received.
        Returns
        -------
        socket : zmq socket
            zmq socket that was constructed.
        '''
        socket = self._context.socket(socketType)
        self._sockets[socket] = handler
        self._poll.register(socket, zmq.POLLIN)

        print('Socket {0} of type {1} ready for input'.format(socket, socketType))
        return socket

    def _poll_once(self, timeout):
        # Poll the sockets using the timeout (milliseconds)
        sockets = dict(self._poll.poll(timeout))
        # Iterate through the returned values
        for socket, state in sockets.iteritems():
            # Execute the handler 
            if state == zmq.POLLIN:
                self._sockets[socket](socket)
            else:
                print('Not dispatching state {0} because it is not {1}'.format(state, zmq.POLLIN))

        self._task_schedule.execute()

    def _handle_response(self, socket):
        ''' Handle response from a request '''
        msg_id, reply_str = socket.recv_multipart()
        if msg_id not in self._requests:
            print('msg_id {0} not in requests'.format(msg_id))
            return

        print('RCV: {0}'.format(reply_str))
        reply = filter_json(json.loads(reply_str))
        self._requests.pop(msg_id).callback(reply)

    def call_later(self, seconds, task):
        ''' Queue a task to be executed after the specified number of seconds have elapsed.
        
        Params
        ------
        seconds : integer
            Number of seconds to wait before executing the task
        task : function
            Function that is to be called to perform the task
        '''
        deferred = defer.Deferred()
        deferred.addCallback(task)
        self._task_schedule.queue_relative_task(seconds, deferred)
