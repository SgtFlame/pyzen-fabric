from zen.fabric.service import Service

class ServiceRegistry(Service):
    ''' Service Registry
    
        Services register themselves with the service registry.  Clients query
        the service registry to determine the location of services.
    '''

    def __init__(self):
        self._services = {}

    def get(self, path):
        if path in self._services:
            response = {
                    'path': path,
                    'addresses': self._services[path],
                }
        else:
            #TODO Error or just return an empty dict?
            response = {'path': path}
        return response
    
    def put(self, path, addresses):
        self._services[path] = addresses
        return {'status' : 'ok'}
