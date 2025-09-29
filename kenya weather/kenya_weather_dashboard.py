# kenya_weather_dashboard.py
import requests
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine, text

class KenyaWeatherDashboard:
    def __init__(self):
        self.weather_api_key = "6997cea907e57f9646887e4baf618c99"
        self.db_config = {
            'host': 'pg-209e851f-velyvineayieta-0621.l.aivencloud.com',
            'port': '18498',
            'database': 'defaultdb',
            'user': 'avnadmin',
            'password': 'AVNS_awyu_iZtPBe8AJq6pjF'
        }
        self.cities = {
            'Nairobi': {'lat': -1.286389, 'lon': 36.817223},
            'Mombasa': {'lat': -4.0435, 'lon': 39.6682},
            'Kisumu': {'lat': -0.1022, 'lon': 34.7617},
            'Eldoret': {'lat': 0.5143, 'lon': 35.2698},
            'Nakuru': {'lat': -0.3031, 'lon': 36.0800}
        }
        self.crop_parameters = {
            'maize': {'min_rain_7d': 20, 'max_rain_7d': 100, 'optimal_temp': (18, 30)},
            'beans': {'min_rain_7d': 25, 'max_rain_7d': 80, 'optimal_temp': (15, 28)},
            'kale': {'min_rain_7d': 15, 'max_rain_7d': 60, 'optimal_temp': (10, 25)},
            'tomatoes': {'min_rain_7d': 20, 'max_rain_7d': 70, 'optimal_temp': (18, 27)}
        }

    def create_database_connection(self):
        try:
            connection_string = f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}?sslmode=require"
            engine = create_engine(connection_string, connect_args={'sslmode': 'require'})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database connected")
            return engine
        except Exception as e:
            print(f"Database connection failed: {e}")
            return None

    def fetch_weather_data(self):
        weather_data = []
        for city, coords in self.cities.items():
            try:
                url = "http://api.openweathermap.org/data/2.5/forecast"
                params = {
                    'lat': coords['lat'],
                    'lon': coords['lon'],
                    'appid': self.weather_api_key,
                    'units': 'metric'
                }
                response = requests.get(url, params=params, timeout=30)
                data = response.json()
                
                for forecast in data['list']:
                    record = {
                        'city': city,
                        'timestamp': forecast['dt_txt'],
                        'temperature': forecast['main']['temp'],
                        'humidity': forecast['main']['humidity'],
                        'wind_speed': forecast['wind']['speed'],
                        'rain_3h': forecast.get('rain', {}).get('3h', 0),
                        'weather_main': forecast['weather'][0]['main']
                    }
                    weather_data.append(record)
                
                print(f"Weather data for {city}: {len(data['list'])} records")
                time.sleep(1)
            except Exception as e:
                print(f"Error for {city}: {e}")
                continue
        
        return pd.DataFrame(weather_data)

    def fetch_product_data(self):
        try:
            url = "https://fakestoreapi.com/products"
            response = requests.get(url, timeout=30)
            products = response.json()
            product_data = []
            
            for product in products:
                record = {
                    'product_id': product['id'],
                    'title': product['title'][:200],
                    'price': float(product['price']),
                    'category': product['category'],
                    'rating_rate': float(product['rating']['rate'])
                }
                product_data.append(record)
            
            print(f"Products fetched: {len(product_data)}")
            return pd.DataFrame(product_data)
        except:
            return self.create_sample_product_data()

    def create_sample_product_data(self):
        products = [
            {'product_id': 1, 'title': 'Laptop', 'price': 799.99, 'category': 'electronics', 'rating_rate': 4.5},
            {'product_id': 2, 'title': 'Smartphone', 'price': 499.99, 'category': 'electronics', 'rating_rate': 4.3},
            {'product_id': 3, 'title': 'Maize Flour', 'price': 2.99, 'category': 'groceries', 'rating_rate': 4.7},
            {'product_id': 4, 'title': 'Fresh Tomatoes', 'price': 1.49, 'category': 'produce', 'rating_rate': 4.8},
        ]
        return pd.DataFrame(products)

    def fetch_customer_order_data(self):
        customer_data = self.create_sample_customer_data()
        order_data = self.create_sample_order_data()
        return customer_data, order_data

    def create_sample_customer_data(self):
        customers = []
        for i in range(1, 101):
            customer = {
                'customer_id': i,
                'first_name': f'Customer{i}',
                'last_name': f'LastName{i}',
                'email': f'customer{i}@example.com',
                'city': np.random.choice(list(self.cities.keys())),
                'registration_date': (datetime.now() - timedelta(days=np.random.randint(1, 365))).strftime('%Y-%m-%d')
            }
            customers.append(customer)
        return pd.DataFrame(customers)

    def create_sample_order_data(self):
        orders = []
        for i in range(1, 201):
            order = {
                'order_id': i,
                'order_date': (datetime.now() - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%d'),
                'customer_id': np.random.randint(1, 101),
                'product_id': np.random.randint(1, 5),
                'quantity': np.random.randint(1, 6),
                'city': np.random.choice(list(self.cities.keys())),
                'status': np.random.choice(['delivered', 'pending', 'cancelled'], p=[0.8, 0.15, 0.05]),
                'delivery_time_minutes': np.random.randint(30, 181)
            }
            orders.append(order)
        return pd.DataFrame(orders)

    def process_weather_metrics(self, weather_df):
        if weather_df.empty:
            return pd.DataFrame()
            
        weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
        weather_df['date'] = weather_df['timestamp'].dt.date
        
        daily_weather = weather_df.groupby(['city', 'date']).agg({
            'temperature': 'mean',
            'wind_speed': 'max',
            'rain_3h': 'sum'
        }).reset_index()
        
        daily_weather.columns = ['city', 'date', 'avg_temp', 'max_wind_speed', 'daily_rainfall']
        
        daily_weather['rain_risk_flag'] = (daily_weather['daily_rainfall'] >= 5).astype(int)
        daily_weather['wind_risk_flag'] = (daily_weather['max_wind_speed'] >= 10).astype(int)
        daily_weather['delivery_risk_index'] = daily_weather['rain_risk_flag'] + daily_weather['wind_risk_flag']
        
        daily_weather['rain_7d_cumulative'] = daily_weather.groupby('city')['daily_rainfall'].transform(
            lambda x: x.rolling(7, min_periods=1).sum()
        )
        
        return daily_weather

    def calculate_operational_kpis(self, orders_df, weather_metrics_df):
        if orders_df.empty:
            return pd.DataFrame()
            
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date']).dt.date
        
        daily_operations = orders_df.groupby(['city', 'order_date']).agg({
            'order_id': 'count',
            'status': lambda x: (x == 'delivered').sum(),
            'delivery_time_minutes': 'mean'
        }).reset_index()
        
        daily_operations.columns = ['city', 'date', 'total_orders', 'delivered_orders', 'avg_delivery_time']
        
        daily_operations['on_time_delivery_rate'] = (
            daily_operations['delivered_orders'] / daily_operations['total_orders'] * 100
        )
        daily_operations['cancellation_rate'] = (
            (daily_operations['total_orders'] - daily_operations['delivered_orders']) / 
            daily_operations['total_orders'] * 100
        )
        
        if not weather_metrics_df.empty:
            result = pd.merge(
                daily_operations, 
                weather_metrics_df[['city', 'date', 'rain_risk_flag', 'wind_risk_flag', 'delivery_risk_index']], 
                on=['city', 'date'], 
                how='left'
            )
            result[['rain_risk_flag', 'wind_risk_flag', 'delivery_risk_index']] = result[['rain_risk_flag', 'wind_risk_flag', 'delivery_risk_index']].fillna(0)
        else:
            result = daily_operations
            result['rain_risk_flag'] = 0
            result['wind_risk_flag'] = 0
            result['delivery_risk_index'] = 0
        
        return result

    def generate_agricultural_advisory(self, weather_metrics_df):
        if weather_metrics_df.empty:
            return pd.DataFrame()
            
        advisory_data = []
        current_date = datetime.now().date()
        
        for city in self.cities.keys():
            city_weather = weather_metrics_df[weather_metrics_df['city'] == city].tail(1)
            if len(city_weather) == 0:
                continue
                
            latest = city_weather.iloc[-1]
            rain = latest['rain_7d_cumulative']
            temp = latest['avg_temp']
            
            for crop, params in self.crop_parameters.items():
                planting_ok = (rain >= params['min_rain_7d'] and 
                             rain <= params['max_rain_7d'] and 
                             params['optimal_temp'][0] <= temp <= params['optimal_temp'][1])
                
                record = {
                    'city': city,
                    'crop': crop,
                    'date': latest['date'],
                    'cumulative_rainfall_7d': rain,
                    'avg_temperature': temp,
                    'planting_recommended': planting_ok
                }
                advisory_data.append(record)
        
        return pd.DataFrame(advisory_data)

    def create_database_tables(self, engine):
        try:
            with engine.connect() as conn:
                tables = [
                    """CREATE TABLE IF NOT EXISTS weather_forecasts (
                        id SERIAL PRIMARY KEY,
                        city VARCHAR(50), date DATE,
                        avg_temp DECIMAL(5,2), max_wind_speed DECIMAL(5,2),
                        daily_rainfall DECIMAL(5,2), rain_risk_flag INTEGER,
                        wind_risk_flag INTEGER, delivery_risk_index INTEGER,
                        rain_7d_cumulative DECIMAL(5,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
                    
                    """CREATE TABLE IF NOT EXISTS products (
                        product_id INTEGER PRIMARY KEY, title VARCHAR(255),
                        price DECIMAL(10,2), category VARCHAR(100),
                        rating_rate DECIMAL(3,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
                    
                    """CREATE TABLE IF NOT EXISTS customers (
                        customer_id INTEGER PRIMARY KEY, first_name VARCHAR(100),
                        last_name VARCHAR(100), email VARCHAR(255), city VARCHAR(50),
                        registration_date DATE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
                    
                    """CREATE TABLE IF NOT EXISTS orders (
                        order_id INTEGER PRIMARY KEY, order_date DATE,
                        customer_id INTEGER, product_id INTEGER, quantity INTEGER,
                        city VARCHAR(50), status VARCHAR(50), delivery_time_minutes INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
                    
                    """CREATE TABLE IF NOT EXISTS operations_kpis (
                        id SERIAL PRIMARY KEY, city VARCHAR(50), date DATE,
                        total_orders INTEGER, delivered_orders INTEGER,
                        avg_delivery_time DECIMAL(10,2), on_time_delivery_rate DECIMAL(5,2),
                        cancellation_rate DECIMAL(5,2), rain_risk_flag INTEGER,
                        wind_risk_flag INTEGER, delivery_risk_index INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
                    
                    """CREATE TABLE IF NOT EXISTS agricultural_advisory (
                        id SERIAL PRIMARY KEY, city VARCHAR(50), crop VARCHAR(100),
                        date DATE, cumulative_rainfall_7d DECIMAL(5,2),
                        avg_temperature DECIMAL(5,2), planting_recommended BOOLEAN,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""
                ]
                
                for table_sql in tables:
                    conn.execute(text(table_sql))
                conn.commit()
                print("Tables created")
        except Exception as e:
            print(f"Table creation error: {e}")

    def load_data_to_database(self, engine, data_frames):
        try:
            with engine.connect() as conn:
                if not data_frames['weather_metrics'].empty:
                    data_frames['weather_metrics'].to_sql('weather_forecasts', conn, if_exists='replace', index=False)
                if not data_frames['products'].empty:
                    data_frames['products'].to_sql('products', conn, if_exists='replace', index=False)
                if not data_frames['customers'].empty:
                    data_frames['customers'].to_sql('customers', conn, if_exists='replace', index=False)
                if not data_frames['orders'].empty:
                    data_frames['orders'].to_sql('orders', conn, if_exists='replace', index=False)
                if not data_frames['operations_kpis'].empty:
                    data_frames['operations_kpis'].to_sql('operations_kpis', conn, if_exists='replace', index=False)
                if not data_frames['agricultural_advisory'].empty:
                    data_frames['agricultural_advisory'].to_sql('agricultural_advisory', conn, if_exists='replace', index=False)
                
                conn.commit()
                print("Data loaded to database")
        except Exception as e:
            print(f"Data loading error: {e}")

    def run_pipeline(self):
        print("Starting pipeline...")
        
        engine = self.create_database_connection()
        if not engine:
            return
        
        self.create_database_tables(engine)
        
        weather_df = self.fetch_weather_data()
        product_df = self.fetch_product_data()
        customer_df, order_df = self.fetch_customer_order_data()
        
        weather_metrics = self.process_weather_metrics(weather_df)
        operations_kpis = self.calculate_operational_kpis(order_df, weather_metrics)
        agricultural_adv = self.generate_agricultural_advisory(weather_metrics)
        
        data_frames = {
            'weather_metrics': weather_metrics,
            'products': product_df,
            'customers': customer_df,
            'orders': order_df,
            'operations_kpis': operations_kpis,
            'agricultural_advisory': agricultural_adv
        }
        
        self.load_data_to_database(engine, data_frames)
        
        print("Pipeline completed")
        print(f"Weather: {len(weather_metrics)} | Products: {len(product_df)}")
        print(f"Customers: {len(customer_df)} | Orders: {len(order_df)}")
        print(f"Operations: {len(operations_kpis)} | Advisory: {len(agricultural_adv)}")

def main():
    dashboard = KenyaWeatherDashboard()
    dashboard.run_pipeline()

if __name__ == "__main__":
    main()