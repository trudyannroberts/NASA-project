from db_mars import create_mars_weather_table, insert_mars_weather
from db_asteroids import create_near_earth_objects_table, insert_near_earth_objects
from api_mars_weather import fetch_mars_data
from api_near_earth_objects import fetch_neo_data

def main():
    # Mars
    mars_data = fetch_mars_data()
    conn, cur = create_mars_weather_table()
    insert_mars_weather(cur, mars_data)
    conn.commit()
    cur.close()
    conn.close()

    # NEO
    neo_data = fetch_neo_data()
    conn, cur = create_near_earth_objects_table()
    insert_near_earth_objects(cur, neo_data)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
