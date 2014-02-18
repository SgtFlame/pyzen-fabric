''' service.py

    Server side service
    
    Extend this class to create a service.
'''

class Service(object):
    
    def __init__(self):
        # Initialize the service container that contains this service as None
        # This will be populated with the correct value when the service
        # is registered with container.register_service(service)
        self._container = None

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
