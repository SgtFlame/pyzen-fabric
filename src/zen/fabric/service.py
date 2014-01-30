''' service.py

    Server side service
    
    Extend this class to create a service.
'''

class Service():
    
    def handle_request(self, client_id, request):
        ''' Handle a request message
            
            It's not necessary to implement this handler.  The default
            implementation will take the 'command' property from the 
            request message and attempt to execute it as a function,
            passing the 'args' property as arguments
        '''
        pass
