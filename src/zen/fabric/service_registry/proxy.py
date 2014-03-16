import json
from twisted.internet   import defer
import zmq

from zen.fabric.service_proxy import ServiceProxy
from zen.fabric import service_registry
from zen.fabric.json_util import filter_json

class ServiceRegistryProxy(ServiceProxy):
    
    def __init__(self, container, srap):
        ''' Initialize the service registry proxy
        
            Params
            =====
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
        #TODO What was the purpose of address_handler?
        #self._address_handler = address_handler
        # { service path : { 'REQ' : service socket, } }
        self._remote_services = {}
        # service path : deferred; this has the deferred objects for all pending 
        # service resolutions
        self._remote_socket_requests = {}
    
    def register_service(self, path, address, port):
        new_request = {
            'path': service_registry.PATH,
            'command': 'put', 
            'args': { 'path': path, 
                            'addresses': { 'REQ': '{0}:{1}'.format(address, port), },
                         }
        }
        # Should this be 
        self._container.send_message_to_socket(self._socket, new_request)

    def get_remote_socket(self, request):
        ''' Gets the socket that is connected to the server that can handle 
            the specified request.

            Returns
            =======
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
        _, reply_str = socket.recv_multipart()
        #TODO I don't like this decoding here; maybe the decoding should be in 
        # the container?
        print('RCV: {0}'.format(reply_str))
        reply = filter_json(json.loads(reply_str))

        if 'path' not in reply:
            # This is a response from  a put; nothing to do
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
        socket = self._container.connect_request(addresses['REQ'])
        print('Connected {2} to service {0} at {1}'.format(service_path, addresses['REQ'], socket))
        service = { 'REQ' : socket, }

        # Index this service by the service path
        self._remote_services[service_path] = service

        # Execute the deferred for the pending requests (and remove it)
        print('Calling back with socket')
        self._remote_socket_requests.pop(service_path).callback(socket)
