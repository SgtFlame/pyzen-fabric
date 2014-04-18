''' service.py

Server side service

Extend this class to create a service.
'''
import json
import traceback

class Service(object):
    
    def __init__(self, container=None):
        ''' Initialize the service container that contains this service as None
        This will be populated with the correct value when the service
        is registered with container.register_service(service)
        (unless, of course, the container is specified here)
        
        Params
        ------
        container - ServiceContainer
            Generally a ServiceContainer, but could be a ServiceEndpoint
            or something derived from either
        '''
        self._container = container

    def handle_request(self, client_id, request):
        ''' Handle a request message
            
            It's not necessary to implement this handler.  The default
            implementation will take the 'command' property from the 
            request message and attempt to execute it as a function,
            passing the 'args' property as arguments
            
            Params
            ======
            client_id : string
                Identifier for this client (should this be session instead?)
            request : dictionary
                Example 
                    { 
                        'path' : '/path/to/this/service', 
                        'command' : 'method_to_invoke', 
                        'args' : { } # args to pass to method
                    }
        '''
        command = request['command']
        try:
            self._client_id = client_id
            func = getattr(self, command)
            response = func(**request['args'])
        except:
            response = { 'status' : 'error', 'message' : traceback.format_exc() }
        finally:
            self._client_id = None

        return response

    def init_notification(self, id):
        socket = self._container.socket(zmq.PUSH, None)
        self._notify[id] = socket
        port = socket.bind_to_random_port('tcp://*')
        return { 'address' : self._container.request_address, 'port' : port }

    def notify(self, id, message):
        ''' Send a notification message to a single client '''
        self._notify[id].send(json.dumps(message))

    def notify_all(self, message):
        ''' Send a notification message to all clients '''
        for socket in self._notify.itervalues():
            socket.send(json.dumps(message))
