import json
import socket as sys_socket
import traceback
import zmq

from zen.fabric.service_endpoint import ServiceEndpoint
from zen.fabric.json_util import filter_json

class ServiceContainer(ServiceEndpoint):
    ''' Service Container

        Container for services.
    '''
    def __init__(self, json_default=None):
        super(ServiceContainer, self).__init__()
        self._request_port = None
        self._services = {}
        self._is_running = False
        self._json_default = json_default

    def init(self, request_address='*', request_port=None, srap=None):
        ''' Initialize the container with the specified request port
        
            Params
            ======
            request_address : string, optional
                Address to which the request socket should bound.  Use '*' as a
                wildcard to bind to all addresses.

            request_port : int, optional
                Port to which the request socket should be bound
            srap : string, optional
                String of address:port where the service registry is located
        '''
        socket = self.socket(zmq.REP, self._handle_request)
        if request_port:
            socket.bind('tcp://{0}:{1}'.format(request_address, request_port))
            self._request_port = request_port
        else:
            bind_address = 'tcp://{0}'.format(request_address)
            print('Binding to {0}'.format(bind_address))
            self._request_port = socket.bind_to_random_port(bind_address)                 
        
        if request_address == '*':
            self._request_address = sys_socket.getfqdn()
        else:
            self._request_address = request_address
        super(ServiceContainer, self).init(srap)
        print('Service container ready on port {0}'.format(self._request_port))

    @property
    def request_address(self):
        return self._request_address

    def register_service(self, service, path, localOnly=False):
        ''' Register a local service with this service registery
        
        Params
        ======
        localOnly : boolean
            True if this service should not be registered with the remote service
            registry.
        '''
        self._services[path] = service
        service._container = self
        if localOnly:
            #TODO Return a Deferred with the callback already called?
            return
        # Register with the remote service registry
        return self._service_registry.register_service(path, self._request_address, self._request_port)

    def _get_service(self, path):
        if path in self._services:
            return self._services[path]
        else:
            return None

    def _handle_request(self, socket):
        ''' Handler for the request port.  This handles inbound request 
            messages.
        '''
        print('in _handle_request')
        msg_id, request_string = socket.recv_multipart()
        print('REQ: {0}'.format(request_string))
        request = filter_json(json.loads(request_string))
        if 'path' not in request:
            #TODO Log error; requests without a path cannot be handled
            print('Error, no path specified.')
            return

        service = self._get_service(request['path'])
        if not service:
            #TODO Log an error, or check to see if  the service exists in a 
            # federated registry or gateway
            print('Error, service path not specified')
            return

        response = service.handle_request(msg_id, request)
        
        if response is None:
            response = {}

        response_string = json.dumps(response, default=self._json_default)
        print('REP: {0}'.format(response_string))
        socket.send_multipart([msg_id, response_string])
