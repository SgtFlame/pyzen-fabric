from zen.fabric.service_endpoint import ServiceEndpoint

class ServiceContainer(ServiceEndpoint):
    ''' Service Container
    
        Container for services.
    '''
    def __init__(self):
        super(ServiceContainer, self).__init__()

    def run(self, pacing=1000):
        ''' Run the service container in the current thread.
        
            pacing - timeout in milliseconds for each poll; this is the
            finest granularity of timeouts and other timed events.
        '''
        while self._is_running:
            self._poll_once(pacing)
