import traceback
import zmq

from zen.fabric.service_endpoint import ServiceEndpoint

class ServiceContainer(ServiceEndpoint):
    ''' Service Container
    
        Container for services.
    '''
    def __init__(self):
        super(ServiceContainer, self).__init__()
        self._request_port = None
        self._services = {}
        self._is_running = False

    def init(self, request_address='*', request_port=None, ):
        ''' Initialize the container with the specified request port
        
            Params
            ======
            request_address : string, optional
                Address to which the request socket should bound.  Use '*' as a
                wildcard to bind to all addresses.

            request_port : int, optional
                Port to which the request socket should be bound
        '''
        socket = self._socket(zmq.REP, self._handle_request)
        if request_port:
            socket.bind('tcp://{0}:{1}'.format(request_address, request_port))
            self._request_port = request_port
        else:
            self._request_port = socket.bind_to_random_port(
                    'tcp://{0}'.format(request_address)
                )

    def register_service(self, service, path):
        ''' Register a local service with this service registery
        '''
        self._services[path] = service
        service._container = self

    def run(self, pacing=1000):
        ''' Run the service container in the current thread.

            Params
            =====
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
                print('Shutting down via keyboard interrupt...')
                self.shutdown()
            except:
                #TODO Handle errors instead of simply ignoring them.
                print('Got an exception:{0}'.format(traceback.format_exc()))
                continue

    def shutdown(self):
        self._is_running = False
        #TODO Anything else need to be done?

    def _handle_request(self, socket):
        ''' Handler for the request port.  This handles inbound request 
            messages.
        '''
        msg_id, request_string = socket.recv_multipart()
        request = filter_json(json.loads(request_string))
        if 'path' not in request:
            #TODO Log error; requests without a path cannot be handled
            return
        
        service = self._get_service(request['path'])
        if not service:
            #TODO Log an error, or check to see if  the service exists in a 
            # federated registry or gateway
            return

        response = service.handle_request(msg_id, request)
        
        if response is None:
            response = {}

        socket.send_multipart([msg_id, json.dumps(response)])
