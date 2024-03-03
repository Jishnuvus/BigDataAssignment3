import requests
import json
import redis
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from db_config import get_redis_connection


class RedisJSONAPI:
    """
    Python Class, that 
        - Reads JSON from API
        - Inserts into RedisJSON
        - Matplotlib charts, Aggregation and Search Operations
    """

    def __init__(self):
        """
        Initialize the Redis connection.
        """
        self.r = get_redis_connection()

    def get_json(self, url):
        """
        Get JSON data from API.

        args: Str - Pass the API URL as a argument

        Returns: dict - API data in a JSON format
        """
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve data from API. Status code: {response.status_code}")
            return None


    def set_into_redis(self, json_data, redis_key):
        """
        Insert JSON data into Redis as a JSON

        args: dict - JSON data and str - redis_key where data is stored
        """
        self.r.json().set(redis_key, '.', json.dumps(json_data))


    def get_from_redis(self, redis_key):
        """
        Retrieve JSON data from Redis.

        Args: str - redis_key where data is stored in Redis

        Return: dict - JSON data retrieved from Redis
        """
        json_data = self.r.json().get(redis_key)
        return json.loads(json_data)

    def search(self, redis_key, year):
        """
        Search for the population by a specific year.

        args: str - redis_key where data is stored in Redis and Year

        Return: Int - Population for the specified year.
        """
        json_data = self.get_from_redis(redis_key)
        if json_data:
            data = json_data.get('data', [])
            for item in data:
                if item['Year'] == year:
                    return item['Population']
        print("No data found for the given year.")
        return None


    def plot_population_years(self, redis_key):
        """
        Plot the population over the years.

        args: str - redis_key where data is stored in Redis
        """
        json_data = self.get_from_redis(redis_key)
        if json_data:
            data = json_data.get('data', [])
            years = [item['Year'] for item in data]
            populations = [item['Population'] for item in data]
            plt.plot(years, populations)
            plt.xlabel('Year')
            plt.ylabel('Population')
            plt.title('Population Over Years')
            plt.show()
        else:
            print("No data found")


    def aggregate(self, redis_key):
        """
        Calculate the total population.

        args: str - redis_key where data is stored in Redis

        Returns: Total population.
        """
        json_data = self.get_from_redis(redis_key)
        if json_data:
            data = json_data.get('data', [])
            total_population = sum([item['Population'] for item in data])
            return total_population
        else:
            print("No data found")
            return 0





