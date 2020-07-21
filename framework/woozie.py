#!./venv/bin/python

# woozie
# an oozie API client and job manager
#


import collections
import time

from api import BaseAPIClient
import darn
import string

#OOZIE_JOBS_ENDPOINT = 'http://h2ms02lax01us.prod.auction.local:11000/oozie/v1/jobs'
REQUEST_PARAMS_START = {'action':'start'}
REQUEST_HEADERS = {'Content-Type':'application/xml'}
RESPONSE_EXPECTED_CONTENT = 'application/json;charset=UTF-8'
REQUEST_ATTEMPTS = 1  # try request x times

CHECK_OFFSET = 1.2
SHORT_OFFSET = 25
LONGER_OFFSET = 40
APPLICATION_SLOT_MODIFIER = 0.5

class OozieClient(BaseAPIClient):
    '''
    An Oozie client.
    Submits jobs via Oozie REST API.
    https://oozie.apache.org/docs/4.0.0/WebServicesAPI.html
    '''
    def __init__(self, OOZIE_JOBS_ENDPOINT):
        BaseAPIClient.__init__(self)
        self.OOZIE_JOBS_ENDPOINT = OOZIE_JOBS_ENDPOINT
        self.endpoint = OOZIE_JOBS_ENDPOINT.replace("'","")

    def run_job(self, configuration, params):
        self._make_request('POST',
                           endpoint=self.endpoint,
                           headers=REQUEST_HEADERS,
                           expected=RESPONSE_EXPECTED_CONTENT,
                           attempts=REQUEST_ATTEMPTS,
                           params=params,
                           data=configuration)


class OozieManager(object):
    '''
    A governor plate for Oozie.
    Controls concurrent Oozie processes to help avoid deadlock due to resource starvation.
    '''

    def __init__(self, user, queue_name, OOZIE_JOBS_ENDPOINT, SCHEDULER_ENDPOINT, METRICS_ENDPOINT):
        self.user = user
        self.queue_name = queue_name
        self.OOZIE_JOBS_ENDPOINT = OOZIE_JOBS_ENDPOINT
        self.SCHEDULER_ENDPOINT = SCHEDULER_ENDPOINT
        self.METRICS_ENDPOINT = METRICS_ENDPOINT
        self.job_queue = _PessimistJobQueue(user, queue_name, OOZIE_JOBS_ENDPOINT, SCHEDULER_ENDPOINT, METRICS_ENDPOINT)
        

    def __enter__(self):
        return self

    def __exit__(self, *err):
        pass

    def submit_job(self, configuration, mapper_count):
        self.job_queue.add_job(configuration, mapper_count)

    def run_queue(self):

        self.job_queue.execute()


class _BaseJobQueue(object):
    '''
    Internal class for OozieManager interface.
    Holds workflows in queue until resource availability condition is met.
    '''

    def __init__(self, user, queue_name, OOZIE_JOBS_ENDPOINT, SCHEDULER_ENDPOINT, METRICS_ENDPOINT):
        self.queue = []
        self.user = user
        self.queue_name = queue_name
        self.OOZIE_JOBS_ENDPOINT = OOZIE_JOBS_ENDPOINT
        self.SCHEDULER_ENDPOINT = SCHEDULER_ENDPOINT
        self.METRICS_ENDPOINT = METRICS_ENDPOINT

    def __enter__(self):
        return self

    def __exit__(self, *err):
        pass

    def _mappers_available(self):
        user = self.user
        queue_name = self.queue_name
        with darn.MetricsClient(METRICS_ENDPOINT=self.METRICS_ENDPOINT) as r:
            total = r.mappers_total()
            available = r.mappers_available()
        with darn.SchedulerClient(SCHEDULER_ENDPOINT=self.SCHEDULER_ENDPOINT) as a:
            factor = a.queue_user_limit_factor(queue_name)
            capacity = a.queue_absolute_capacity(queue_name)
            used = a.user_mappers_used(user, queue_name)
        user_max = total * ((capacity * factor) / 100)
        return min(available, int(user_max - used))

    def _applications_available(self):
        user = self.user
        queue_name = self.queue_name
        with darn.SchedulerClient(SCHEDULER_ENDPOINT=self.SCHEDULER_ENDPOINT) as a:
            app_limit = a.queue_max_applications_per_user(queue_name)
            user_apps = a.user_active_applications(user, queue_name)
        return app_limit - user_apps

    def add_job(self, configuration, mapper_count):
        queue = self.queue
        Job = collections.namedtuple('Job', 'configuration mapper_count')
        t = Job(configuration, int(mapper_count))
        queue.append(t)

class _PessimistJobQueue(_BaseJobQueue):
    '''
    A job queue for tiny clusters. Pessimist job queue assumes glass is half-empty.
    Pessimist job queue heavily limits user's concurrent applications and mapper usage,
    and waits several seconds between job submission.
    '''

    def __init__(self, user, queue_name, OOZIE_JOBS_ENDPOINT, SCHEDULER_ENDPOINT, METRICS_ENDPOINT):
        self.queue = []
        self.user = user
        self.queue_name = queue_name
        self.OOZIE_JOBS_ENDPOINT = OOZIE_JOBS_ENDPOINT
        self.SCHEDULER_ENDPOINT = SCHEDULER_ENDPOINT
        self.METRICS_ENDPOINT = METRICS_ENDPOINT

    def _check_availability(self, times):
        # check number of available map slots x times, take the min
        # between checks, wait for the check_offset
        mapper_result = []
        application_result = []
        for i in range(0, times):
            mapper_result.append(self._mappers_available())
            application_result.append(self._applications_available())
            if i < times: time.sleep(CHECK_OFFSET)
        mappers = min(mapper_result)
        applications = min(application_result)
        return mapper_result, application_result, mappers, applications

    def execute(self):
        self.queue.sort(key=lambda x:x.mapper_count, reverse=True)
        queue = self.queue
        done = False
        while not done:

            mapper_result, application_result, mappers, applications \
                = self._check_availability(times=5)
            application_throttle = int(applications * APPLICATION_SLOT_MODIFIER)
            # find the first job that fits in the available map slots
            first_job_that_fits = next((job for job in queue if job.mapper_count < mappers), None)

            print 'mapper_result0: %d mapper_result1: %d ' % (mapper_result[0], mapper_result[1])
            print 'application_result0: %d application_result1: %d' % (application_result[0], application_result[1])
            print 'application_limit: %d' % application_throttle
            print first_job_that_fits

            if first_job_that_fits and applications > application_throttle:
                with OozieClient(OOZIE_JOBS_ENDPOINT=self.OOZIE_JOBS_ENDPOINT) as c:
                    c.run_job(configuration=first_job_that_fits.configuration, params=REQUEST_PARAMS_START)
                queue.remove(first_job_that_fits)
                if not queue:
                    done = True
                else:
                    time.sleep(SHORT_OFFSET)
            else:
                # there's no job in the queue that fits
                time.sleep(LONGER_OFFSET)

# if __name__ == '__main__':
#     with _PessimistJobQueue() as m:
#         print m._mappers_available()
