#!./venv/bin/python

# api.py
# wraps requests for interacting with json APIs


import requests
import sys
import time

REQUEST_ATTEMPTS = 3  # try request x times
REQUEST_STALE_MODIFIER = 0.5  # get fresh response when x seconds old
RESPONSE_EXPECTED_CONTENT = 'application/json' # assume json response, this is true majority of time

class BaseAPIClient(object):

    def __init__(self):
        self.response = None
        self.response_timestamp = None
        self.response_json = None

    def __enter__(self):
        return self

    def __exit__(self, *err):
        pass

    def _make_request(self,
                      method,
                      endpoint,
                      attempts=REQUEST_ATTEMPTS,
                      stale=REQUEST_STALE_MODIFIER,
                      expected=RESPONSE_EXPECTED_CONTENT,
                      **kwargs):
        '''
        method for making GET requests to REST API
        :param url: API endpoint URL
        '''
        if self.response_timestamp == None \
                or time.time() - self.response_timestamp > REQUEST_STALE_MODIFIER:
            attempt = 0
            while attempt < REQUEST_ATTEMPTS:
                try:
                    self.response = requests.request(method=method, url=endpoint, **kwargs)
                    # error if status code != 200
                    self.response.raise_for_status()
                    # error if response is not json
                    actual = self.response.headers['content-type']
                    if actual != expected:
                        error_string = 'Wrong type: expected %s, got %s' % (expected, actual)
                        raise requests.exceptions.RequestException(error_string)
                except:
                    e = sys.exc_info()[0]
                    print 'Failed attempt %d : %s' % (attempt, e)
                    if attempt + 1 == REQUEST_ATTEMPTS:
                        raise
                    attempt += 1
                else:
                    self.response_json = self._parse_response()
                    self.response_timestamp = time.time()
                    break

    def _parse_response(self):
        '''
        parses API's JSON response
        :return: parsed response as dict, or none
        '''
        try:
            response_json = self.response.json()
        except:
            return None
        else:
            return response_json
