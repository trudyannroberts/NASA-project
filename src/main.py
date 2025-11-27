from src.db.db_mars_weather import create_mars_weather_table, insert_mars_weather
from src.db.db_neo import create_near_earth_objects_table, insert_near_earth_objects
from src.db.db_picture import create_space_picture_table, insert_space_picture
from src.api.api_mars_weather import fetch_mars_data
from src.api.api_neo import fetch_neo_data
from src.api.api_picture import fetch_picture_data

def main():
    # Mars
    create_mars_weather_table()
    insert_mars_weather(fetch_mars_data())

    # NEO
    create_near_earth_objects_table()
    insert_near_earth_objects(fetch_neo_data())

    # Picture of the day
    create_space_picture_table()
    insert_space_picture(*fetch_picture_data())

if __name__ == "__main__":
    main()
