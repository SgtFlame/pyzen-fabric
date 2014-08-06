from zen.fabric.service import Service
from zen.fabric import service_registry
from zen.fabric.batch_service_container import BatchServiceContainer

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

# Main function that run this service in a standalone process
def main():
    import argparse
    # Get arguments
    arg_parser = argparse.ArgumentParser(description='Run the Data Registry')
    arg_parser.add_argument('--port', default=8888, help='Service Registry port')

    args = arg_parser.parse_args()

    # Run Main
    # Create service container and service(s)
    container = BatchServiceContainer()
    container.init(request_port=args.port)

    registry = ServiceRegistry()

    # The service registry is always only a local service because it shouldn't 
    # be registered with another service registry (at least not until there are
    # redundant service registries)
    container.register_service(registry, service_registry.PATH, True)

    container.run()

if __name__ == "__main__":
    main()
