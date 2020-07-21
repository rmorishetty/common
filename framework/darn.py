#!./venv/bin/python

# darn.py
# a YARN API tool
#
# author:   jstephens
# created:  2/12/2016
# modified: mdurisheti (2016-11-18) Made the code more easily configurable by 
#           moving the hard coded values to the calling scripts

from api import BaseAPIClient
import string

#METRICS_ENDPOINT = 'http://h2ms01lax01us.prod.auction.local:8088/ws/v1/cluster/metrics'
#SCHEDULER_ENDPOINT = 'http://h2ms01lax01us.prod.auction.local:8088/ws/v1/cluster/scheduler'
MAP_CONTAINER_MEM = 4096.0  # 4GB per mapper

# APPS_ENDPOINT = 'http://h2ms01lax01us.prod.auction.local:8088/ws/v1/cluster/apps'

# class ApplicationClient(BaseAPIClient):
#     def __init__(self):
#         BaseAPIClient.__init__(self)
#         self.endpoint = APPS_ENDPOINT
#
#     def user_applications(self, params):
#         self._make_request('GET', endpoint=self.endpoint, params=params)
#         response = self.response_json
#         try:
#             apps = response['apps']['app']
#         except:
#             return 0
#         else:
#             return len(apps)

class SchedulerClient(BaseAPIClient):
    def __init__(self, SCHEDULER_ENDPOINT):
        BaseAPIClient.__init__(self)
        self.SCHEDULER_ENDPOINT = SCHEDULER_ENDPOINT
        self.endpoint = SCHEDULER_ENDPOINT.replace("'","")

    def _get_scheduler_info(self):
        self._make_request('GET', endpoint=self.endpoint)
        scheduler = self.response_json
        return scheduler['scheduler']['schedulerInfo']

    def _get_queue_by_name(self, name):
        self._make_request('GET', endpoint=self.endpoint)
        scheduler = self.response_json
        queues = scheduler['scheduler']['schedulerInfo']['queues']['queue']
        queue = None
        for item in queues:
            if item['queueName'] == name:
                queue = item
                break
        return queue

    def _get_user(self, user_name, queue_name):
        queue = self._get_queue_by_name(queue_name)
        users = queue['users']['user']
        user = None
        for item in users:
            if item['username'] == user_name:
                user = item
                break
        return user

    def cluster_used_capacity(self):
        scheduler = self._get_scheduler_info()
        return scheduler['usedCapacity']

    def queue_capacity(self, name):
        queue = self._get_queue_by_name(name)
        return queue['capacity']

    def queue_max_capacity(self, name):
        queue = self._get_queue_by_name(name)
        return queue['maxCapacity']

    def queue_used_capacity(self, name):
        queue = self._get_queue_by_name(name)
        return queue['usedCapacity']

    def queue_absolute_capacity(self, name):
        queue = self._get_queue_by_name(name)
        return queue['absoluteCapacity']

    def queue_absolute_max_capacity(self, name):
        queue = self._get_queue_by_name(name)
        return queue['absoluteMaxCapacity']

    def queue_absolute_used_capacity(self, name):
        queue = self._get_queue_by_name(name)
        return queue['absoluteUsedCapacity']

    def queue_num_applications(self, name):
        queue = self._get_queue_by_name(name)
        return queue['numApplications']

    def queue_resources_used(self, name):
        queue = self._get_queue_by_name(name)
        return queue['resourcesUsed']

    def queue_max_applications_per_user(self, name):
        queue = self._get_queue_by_name(name)
        return queue['maxApplicationsPerUser']

    def queue_user_limit_factor(self, name):
        queue = self._get_queue_by_name(name)
        return queue['userLimitFactor']

    def queue_users(self, name):
        queue = self._get_queue_by_name(name)
        return queue['users']

    def user_active_applications(self, user_name, queue_name):
        try:
            user = self._get_user(user_name, queue_name)
            apps = user['numActiveApplications']
        except:
            return 0
        else:
            return apps

    def user_mem_used(self, user_name, queue_name):
        try:
            user = self._get_user(user_name, queue_name)
            mem = user['resourcesUsed']['memory']
        except:
            return 0
        else:
            return mem

    def user_mappers_used(self, user_name, queue_name):
        return self.user_mem_used(user_name=user_name, queue_name=queue_name) / MAP_CONTAINER_MEM

    def user_vcores_used(self, user_name, queue_name):
        try:
            user = self._get_user(user_name, queue_name)
            vcores = user['resourcesUsed']['vCores']
        except:
            return 0
        else:
            return vcores


class MetricsClient(BaseAPIClient):
    '''class to check status of YARN resources
    '''
    def __init__(self, METRICS_ENDPOINT):
        BaseAPIClient.__init__(self)
        self.METRICS_ENDPOINT = METRICS_ENDPOINT
        self.endpoint = METRICS_ENDPOINT.replace("'","")

    def _get_value(self, key):
        '''
        refresh response and look up an individual value by key
        :param key: key to look up in dict
        :return: value of key
        '''
        self._make_request('GET', endpoint=self.endpoint)
        return self.response_json['clusterMetrics'][key]

    def containers_allocated(self):
        return self._get_value('containersAllocated')

    def containers_pending(self):
        return self._get_value('containersPending')

    def containers_reserved(self):
        return self._get_value('containersReserved')

    def mappers_allocated(self):
        return self._get_value('allocatedMB') / MAP_CONTAINER_MEM

    def mappers_total(self):
        return self._get_value('totalMB') / MAP_CONTAINER_MEM

    def mappers_available(self):
        return self._get_value('availableMB') / MAP_CONTAINER_MEM

    def mem_available(self):
        return self._get_value('availableMB')

    def mem_reserved(self):
        return self._get_value('reservedMB')

    def mem_allocated(self):
        return self._get_value('allocatedMB')

    def nodes_active(self):
        return self._get_value('activeNodes')

    def nodes_unhealthy(self):
        return self._get_value('unhealthyNodes')


# if __name__ == '__main__':
    # with MetricsClient() as r:
    #     print 'containers_allocated: %d' % r.containers_allocated()
    #     print 'containers_pending: %d' % r.containers_pending()
    #     print 'containers_reserved: %d' % r.containers_reserved()
    #     print 'map_slots_allocated: %.2f' % r.mappers_allocated()
    #     print 'map_slots_available: %d' % r.mappers_available()
    #     print 'map_slots_total: %d' % r.mappers_total()
    #     print 'mem_allocated: %d' % r.mem_allocated()
    #     print 'mem_available: %d' % r.mem_available()
    #     print 'mem_reserved: %d' % r.mem_reserved()
    #     print 'nodes_active: %d' % r.nodes_active()
    #     print 'nodes_unhealthy: %d' % r.nodes_unhealthy()

    # params = {'user':USER_NAME, 'states':'RUNNING,ACCEPTED'}
    # with ApplicationClient() as a:
    #     print 'user_applications: %s' % a.user_applications(params=params)

    # with SchedulerClient() as a:
    #     print 'cluster_used_capacity: %.2f' % a.cluster_used_capacity()
    #     print 'queue_max_applications_per_user: %d' % a.queue_max_applications_per_user(QUEUE_NAME)
    #     print 'queue_user_limit_factor: %.2f' % a.queue_user_limit_factor(QUEUE_NAME)
    #     print 'queue_absolute_capacity: %.2f' % a.queue_absolute_capacity(QUEUE_NAME)
    #     print 'queue_absolute_max_capacity: %.2f' % a.queue_absolute_max_capacity(QUEUE_NAME)
    #     print 'queue_absolute_used_capacity: %.2f' % a.queue_absolute_used_capacity(QUEUE_NAME)
    #     print 'queue_capacity: %.2f' % a.queue_capacity(QUEUE_NAME)
    #     print 'queue_max_capacity: %.2f' % a.queue_max_capacity(QUEUE_NAME)
    #     print 'queue_used_capacity: %.2f' % a.queue_used_capacity(QUEUE_NAME)
    #     print 'queue_num_applications: %d' % a.queue_num_applications(QUEUE_NAME)
    #     print 'queue_resources_used: %s' % a.queue_resources_used(QUEUE_NAME)
    #     print 'queue_users: %s' % a.queue_users(QUEUE_NAME)
    #     print 'user_mem_used: %d' % a.user_mem_used('nickd', QUEUE_NAME)
    #     print 'user_mappers_used: %.2f' % a.user_mappers_used('nickd', QUEUE_NAME)
    #     print 'user_active_applications: %d' % a.user_active_applications('nickd', QUEUE_NAME)
