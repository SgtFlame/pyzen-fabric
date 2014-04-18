from twisted.internet import defer
import uuid

class ServiceProxy(object):
    
    def __init__(self, container):
        self._container = container
        self._pending = []
        self._notification_id = uuid.uuid4().hex

    def init_notification(self):
        ''' In order to use this function, the derived proxy must 
        implement a property named 'path'
        
        '''
        request = {
            'command' : 'init_notification',
            'path' : self.path,
            'args' : { 'id', self._notification_id }
        }
        deferred = self.queue_request(request)
        deferred.addCallback(self._notification_details)
        return deferred
        
    def _notification_details(self, response):
        self._notification_socket = self._container.socket(zmq.PULL, self._got_notification)
        self._notification_socket.connect('tcp://{0}:{1}'.format(response['address'], response['port']))
        
    def queue_request(self, request):
        ''' Use this instead of directly calling self._container.send_request()
        
        This prevents multiple requests from being queued to the same service
        without waiting for an intervening reply (which isn't legal in zmq).
        
        Params
        ------
        request : dictionary
            Standard request message that contains a path, command, and
            args entries.

        Returns
        -------
        reply_received : defer.Deferred
            Deferred object that is fired when the reply to the request
            has been received.
        '''
        reply_received = defer.Deferred()

        self._pending.append((request, reply_received))
        
        if len(self._pending) == 1:
            deferred = self._container.send_request(request)
            deferred.addCallback(self._check_queue)
        
        return reply_received

    def _check_queue(self, reply):
        # Pop the first message from the queue
        org_request, reply_received = self._pending.pop(0)
        
        # Send the next request (if there is one)
        if self._pending:
            deferred = self._container.send_request(self._pending[0][0])
            deferred.addCallback(self._check_queue)
        
        # Callback with the reply
        reply_received.callback(reply)

