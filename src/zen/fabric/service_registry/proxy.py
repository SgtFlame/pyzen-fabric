import json
from twisted.internet   import defer
import uuid
import zmq

from zen.fabric.service_proxy import ServiceProxy
from zen.fabric import service_registry
from zen.fabric.json_util import filter_json

class ServiceRegistryProxy(ServiceProxy):
    
    def __init__(self, container, srap):
        ''' Initialize the service registry proxy
        
        Params
        ------
        container : ServiceContainer
            Service Container or Service Endpoint
        srap : string
            Service registry address:port.
        '''
        super(ServiceRegistryProxy, self).__init__(container)
        self._socket = self._container.socket(zmq.REQ, self._handle_response)
        if srap:
            #TODO Log
            print('Connecting to service registry {0}'.format(srap))
            self._socket.connect('tcp://{0}'.format(srap))
        # _remote_services : { service path : { 'REQ' : service socket, } }
        self._remote_services = {}
        # _remote_socket_requests : { service path : deferred }
        # this has the deferred objects for all pending service resolutions
        self._remote_socket_requests = {}
        # Deferred for pending registrations indexed by msg_id
        self._pending_registrations = {}
    
    def register_service(self, path, address, port):
        ''' Register a service with the service registry
        
        Params
        ------
        path - string
            Path to the service; the service registry uniquely identifies 
            services through the use of this path.
        address - string
            Address where the service is located, generally the fully qualified
            domain name of the machine on which this process is running
        port - string or integer
            Port where the service is located
        '''
        new_request = {
            'path': service_registry.PATH,
            'command': 'put', 
            'args': { 'path': path, 
                            'addresses': { 'REQ': '{0}:{1}'.format(address, port), },
                         }
        }
        # Register with the service registry
        msg_id = uuid.uuid4().hex
        self._container.send_message_to_socket(self._socket, new_request, msg_id=msg_id)
        deferred = defer.Deferred()
        self._pending_registrations[msg_id] = deferred
        return deferred

    def get_remote_socket(self, request):
        ''' Gets the socket that is connected to the server that can handle 
        the specified request.

        Params
        ------
        request : dictionary
            Standard request dictionary with command, path, args entries.

        Returns
        -------
        got_remote_service : defer.Deferred
            This is an asynchronous call, so it returns a Deferred object
            that will signal when the socket is known.
        '''
        path = request['path']

        # First check the cache
        if path in self._remote_services:
            socket = self._remote_services[path]['REQ']
            print('Already got socket for {0} - {1}'.format(path, socket))
            got_remote_socket = defer.Deferred()
            got_remote_socket.callback(socket)
            return got_remote_socket
        # Next check to see if a request for this socket has already been sent
        elif path in self._remote_socket_requests:
            # Duplicate request, send the original deferred
            print('Duplicate request, returning same deferred')
            return self._remote_socket_requests[path]
        # Send the request to the service registry
        else:
            got_remote_socket = defer.Deferred()
            new_request = {
                'path': service_registry.PATH,
                'command': 'get',
                'args': { 'path': path, },
            }
            self._remote_socket_requests[path] = got_remote_socket
            self._container.send_message_to_socket(self._socket, new_request)
            return got_remote_socket

    def _handle_response(self, socket):
        msg_id, reply_str = socket.recv_multipart()
        #TODO I don't like this decoding here; maybe the decoding should be in 
        # the container?
        print('RCV: {0}'.format(reply_str))
        reply = filter_json(json.loads(reply_str))

        if 'path' not in reply:
            # This is a response from a put
            if msg_id in self._pending_registrations:
                self._pending_registrations.pop(msg_id).callback(reply)
            return

        service_path = reply['path']

        if 'addresses' not in reply:
            # Either the service registry doesn't know about the service, or
            # this is a response to a 'put' request.
            #TODO Handle unknown services?  Errback instead of callback?
            self._remote_socket_requests.pop(service_path).callback(None)
            return

        addresses = reply['addresses']
        
        #TODO Handle other connection types
        connect_string = addresses['REQ']
        socket = self._container.connect_request(connect_string)
        print('Connected {2} to service {0} at {1}'.format(service_path, addresses['REQ'], socket))
        service = { 'REQ' : socket, }

        # Index this service by the service path
        self._remote_services[service_path] = service

        # Execute the deferred for the pending requests (and remove it)
        print('Calling back with socket')
        self._remote_socket_requests.pop(service_path).callback(socket)
