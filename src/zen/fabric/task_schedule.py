from bisect import bisect_right
from datetime import timedelta, datetime

class TaskSchedule():
    ''' Task Schedule
    
        Keeps track of tasks that need to be performed at a specific
        time, and then executes the task at the appropriate time.
    '''
    def __init__(self):
        # Ordered list of times when tasks are scheduled to execute
        self._task_times = []
        # Ordered list of tasks, associated with the _task_times array
        self._tasks =[]
    
    def queue_relative_task(self, seconds, task):
        ''' Queue a task to be executed in the specified number of 
            seconds.
            
            task - must be an object with "callback" as a function;
                e.g. twisted Deferred
        '''
        k = datetime.now() + timedelta(seconds=seconds)
        return self.queue_absolute_task(k, task)

    def queue_absolute_task(self, time, task):
        ''' Queue a task to be executed at the specified time
        
            time - datetime object (or something that can be compared
                with datetime.now())
            task - must be an object with "callback" as a function;
                e.g. twisted Deferred
        '''
        i = bisect_right(time, self._taskTimes)
        self._taskTimes.insert(i, time)
        self._tasks.insert(i, task)
        return i
    
    def execute(self):
        ''' Executes all of the tasks that need to be executed.
        '''
        while self._task_times and self._task_times[0] <= datetime.now():
            self._tasks.pop(0).callback()
            self._task_times(0)
