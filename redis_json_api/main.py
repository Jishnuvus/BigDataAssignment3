
from redis_json_api import RedisJSONAPI

if __name__ == "__main__":
    redis_data = RedisJSONAPI()
    json_data = redis_data.get_json("https://datausa.io/api/data?drilldowns=Nation&measures=Population")
    if json_data:
        redis_data.set_into_redis(json_data, "redis_key")
        print("JSON data inserted into Redis successfully.")
        # Perform processing tasks
        # Aggregation Operation
        total_population = redis_data.aggregate("redis_key")
        print(f"Aggregation Operation - Total population: {total_population}")
        # Search Operation
        population_2021 = redis_data.search("redis_key", '2021')
        print(f"Search Operation - Population in 2021: {population_2021}")
        # Matplotlib charts
        redis_data.plot_population_years("redis_key")
    else:
        print("Failed to retrieve JSON data from the API.")
