import logging
import requests


def try_request(url, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.Timeout:
        logging.error('The request timed out')
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f'An error occurred: {e}')
        raise
