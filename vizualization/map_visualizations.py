import json
import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap, MarkerCluster
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.spatial.distance import cdist
import warnings
warnings.filterwarnings('ignore')

# NYC center coordinates
NYC_CENTER = [40.7128, -74.0060]

def load_dataset(filename='mock_joined_dataset.json'):
    """Load the mock dataset from JSON file"""
    with open(filename, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def preprocess_data(df):
    """Preprocess the dataframe for mapping"""
    # Convert numeric columns
    numeric_cols = ['number_of_persons_injured', 'number_of_persons_killed',
                    'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
                    'number_of_cyclist_injured', 'number_of_cyclist_killed',
                    'number_of_motorist_injured', 'number_of_motorist_killed',
                    'temperature', 'feels_like', 'humidity', 'pressure',
                    'wind_speed', 'wind_deg', 'clouds', 'visibility',
                    'latitude', 'longitude']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Create severity score
    df['severity_score'] = (
        df['number_of_persons_injured'] * 1 +
        df['number_of_persons_killed'] * 10 +
        df['number_of_pedestrians_injured'] * 1.5 +
        df['number_of_pedestrians_killed'] * 15
    )
    
    # Create weather severity score
    weather_scores = {
        'Clear': 1,
        'Clouds': 2,
        'Mist': 3,
        'Fog': 4,
        'Rain': 5,
        'Snow': 6
    }
    df['weather_severity_score'] = df['weather_main'].map(weather_scores).fillna(1)
    
    # Filter out invalid coordinates
    df = df[(df['latitude'] >= 40.4) & (df['latitude'] <= 41.0)]
    df = df[(df['longitude'] >= -74.3) & (df['longitude'] <= -73.7)]
    
    return df

def create_collision_heatmap(df):
    """Create heatmap of collisions on NYC map"""
    print("Creating collision heatmap...")
    
    # Create base map
    m = folium.Map(location=NYC_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    # Prepare heatmap data: [lat, lon, weight]
    heat_data = []
    for idx, row in df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Weight by severity (injuries + fatalities)
            weight = row['severity_score'] if pd.notna(row['severity_score']) else 1
            heat_data.append([row['latitude'], row['longitude'], weight])
    
    # Add heatmap layer
    HeatMap(
        heat_data,
        min_opacity=0.2,
        max_zoom=18,
        radius=15,
        blur=10,
        gradient={
            0.2: 'blue',
            0.4: 'cyan',
            0.6: 'lime',
            0.8: 'yellow',
            1.0: 'red'
        }
    ).add_to(m)
    
    # Add markers for high-severity collisions
    high_severity = df[df['severity_score'] > df['severity_score'].quantile(0.75)]
    marker_cluster = MarkerCluster().add_to(m)
    
    for idx, row in high_severity.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            popup_text = f"""
            <b>Collision ID:</b> {row['collision_id']}<br>
            <b>Date:</b> {row['crash_date']}<br>
            <b>Time:</b> {row['crash_time']}<br>
            <b>Borough:</b> {row['borough']}<br>
            <b>Injured:</b> {row['number_of_persons_injured']}<br>
            <b>Killed:</b> {row['number_of_persons_killed']}<br>
            <b>Severity Score:</b> {row['severity_score']:.1f}
            """
            folium.Marker(
                [row['latitude'], row['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                icon=folium.Icon(color='red', icon='exclamation-triangle')
            ).add_to(marker_cluster)
    
    # Add title
    title_html = '''
    <h3 align="center" style="font-size:20px"><b>NYC Collisions Heatmap</b></h3>
    <p align="center">Red areas indicate higher collision severity (injuries + fatalities)</p>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    m.save('nyc_collisions_heatmap.html')
    print("✓ Saved: nyc_collisions_heatmap.html")
    return m

def create_weather_heatmap(df):
    """Create heatmap of weather conditions on NYC map"""
    print("Creating weather/climate heatmap...")
    
    # Create base map
    m = folium.Map(location=NYC_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    # Prepare heatmap data based on weather severity
    heat_data = []
    for idx, row in df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Weight by weather severity and temperature
            weather_weight = row['weather_severity_score'] if pd.notna(row['weather_severity_score']) else 1
            # Combine with visibility (lower visibility = higher weight)
            visibility_factor = 1 / (row['visibility'] / 10000) if pd.notna(row['visibility']) else 1
            weight = weather_weight * visibility_factor
            heat_data.append([row['latitude'], row['longitude'], weight])
    
    # Add heatmap layer
    HeatMap(
        heat_data,
        min_opacity=0.3,
        max_zoom=18,
        radius=20,
        blur=12,
        gradient={
            0.2: 'lightblue',
            0.4: 'cyan',
            0.6: 'yellow',
            0.8: 'orange',
            1.0: 'darkred'
        }
    ).add_to(m)
    
    # Add markers for severe weather conditions
    severe_weather = df[df['weather_severity_score'] >= 4]  # Rain, Snow, Fog
    marker_cluster = MarkerCluster().add_to(m)
    
    weather_icons = {
        'Rain': 'tint',
        'Snow': 'snowflake-o',
        'Fog': 'cloud',
        'Mist': 'cloud'
    }
    
    for idx, row in severe_weather.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            icon_name = weather_icons.get(row['weather_main'], 'info')
            popup_text = f"""
            <b>Weather:</b> {row['weather_main']}<br>
            <b>Description:</b> {row['weather_description']}<br>
            <b>Temperature:</b> {row['temperature']:.1f}°C<br>
            <b>Humidity:</b> {row['humidity']}%<br>
            <b>Visibility:</b> {row['visibility']}m<br>
            <b>Wind Speed:</b> {row['wind_speed']:.1f} m/s<br>
            <b>Date:</b> {row['crash_date']}
            """
            folium.Marker(
                [row['latitude'], row['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                icon=folium.Icon(color='blue', icon=icon_name, prefix='fa')
            ).add_to(marker_cluster)
    
    # Add title
    title_html = '''
    <h3 align="center" style="font-size:20px"><b>NYC Weather/Climate Heatmap</b></h3>
    <p align="center">Red areas indicate severe weather conditions (rain, snow, fog, low visibility)</p>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    m.save('nyc_weather_heatmap.html')
    print("✓ Saved: nyc_weather_heatmap.html")
    return m

def create_events_heatmap(df):
    """Create heatmap of events on NYC map"""
    print("Creating events heatmap...")
    
    # Filter to only events
    events_df = df[df['event_id'].notna()].copy()
    
    if len(events_df) == 0:
        print("⚠ No events found in dataset")
        return None
    
    # Create base map
    m = folium.Map(location=NYC_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    # Prepare heatmap data
    # For events, we'll use the collision locations as proxy (since events don't have separate coordinates)
    # Weight by event type and duration
    heat_data = []
    for idx, row in events_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Weight by event type (sports events might have more impact)
            event_weight = 2 if 'Sport' in str(row['event_type']) else 1
            # Add weight for street closures
            if pd.notna(row['street_closure_type']) and row['street_closure_type'] != 'N/A':
                event_weight += 1
            heat_data.append([row['latitude'], row['longitude'], event_weight])
    
    # Add heatmap layer
    HeatMap(
        heat_data,
        min_opacity=0.2,
        max_zoom=18,
        radius=18,
        blur=10,
        gradient={
            0.2: 'green',
            0.4: 'lime',
            0.6: 'yellow',
            0.8: 'orange',
            1.0: 'red'
        }
    ).add_to(m)
    
    # Add markers for all events
    marker_cluster = MarkerCluster().add_to(m)
    
    event_icons = {
        'Sport - Adult': 'futbol-o',
        'Sport - Youth': 'futbol-o',
        'Special Event': 'calendar',
        'Street Fair': 'shopping-cart',
        'Parade': 'users',
        'Cultural Event': 'music'
    }
    
    for idx, row in events_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            icon_name = event_icons.get(row['event_type'], 'calendar')
            popup_text = f"""
            <b>Event ID:</b> {row['event_id']}<br>
            <b>Event Name:</b> {row['event_name']}<br>
            <b>Event Type:</b> {row['event_type']}<br>
            <b>Agency:</b> {row['event_agency']}<br>
            <b>Location:</b> {row['event_location']}<br>
            <b>Start:</b> {row['event_start_date_time']}<br>
            <b>End:</b> {row['event_end_date_time']}<br>
            <b>Street Closure:</b> {row['street_closure_type']}<br>
            <b>Borough:</b> {row['event_borough']}
            """
            folium.Marker(
                [row['latitude'], row['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                icon=folium.Icon(color='green', icon=icon_name, prefix='fa')
            ).add_to(marker_cluster)
    
    # Add title
    title_html = '''
    <h3 align="center" style="font-size:20px"><b>NYC Permitted Events Heatmap</b></h3>
    <p align="center">Red areas indicate high event density or events with street closures</p>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    m.save('nyc_events_heatmap.html')
    print("✓ Saved: nyc_events_heatmap.html")
    return m

def create_correlation_heatmaps(df):
    """Create correlation heatmaps for different variable combinations"""
    print("Creating correlation heatmaps...")
    
    # Prepare data for correlation analysis
    correlation_data = df.copy()
    
    # Create binary flags
    correlation_data['has_event'] = correlation_data['event_id'].notna().astype(int)
    correlation_data['has_injuries'] = (correlation_data['number_of_persons_injured'] > 0).astype(int)
    correlation_data['has_fatalities'] = (correlation_data['number_of_persons_killed'] > 0).astype(int)
    
    # 1. Collision-Weather Correlation Heatmap
    fig, axes = plt.subplots(1, 3, figsize=(20, 6))
    fig.suptitle('Correlation Heatmaps: Collisions, Weather, and Events', fontsize=16, fontweight='bold')
    
    # Collision-Weather correlation
    collision_weather_cols = [
        'number_of_persons_injured', 'number_of_persons_killed',
        'temperature', 'humidity', 'wind_speed', 'visibility',
        'weather_severity_score', 'pressure', 'clouds'
    ]
    corr1 = correlation_data[collision_weather_cols].corr()
    sns.heatmap(corr1, annot=True, fmt='.2f', cmap='RdYlGn_r', center=0,
               square=True, linewidths=1, cbar_kws={"shrink": 0.8}, ax=axes[0])
    axes[0].set_title('Collision-Weather Correlation', fontweight='bold', fontsize=12)
    
    # Collision-Event Correlation
    collision_event_cols = [
        'number_of_persons_injured', 'number_of_persons_killed',
        'has_event', 'severity_score'
    ]
    # Add event-related numeric columns if they exist
    event_numeric = []
    if 'event_id' in correlation_data.columns:
        # Create event count per borough
        event_counts = correlation_data.groupby('borough')['has_event'].sum()
        correlation_data['event_count'] = correlation_data['borough'].map(event_counts)
        event_numeric.append('event_count')
    
    collision_event_cols.extend(event_numeric)
    corr2 = correlation_data[collision_event_cols].corr()
    sns.heatmap(corr2, annot=True, fmt='.2f', cmap='RdYlGn_r', center=0,
               square=True, linewidths=1, cbar_kws={"shrink": 0.8}, ax=axes[1])
    axes[1].set_title('Collision-Event Correlation', fontweight='bold', fontsize=12)
    
    # Combined Correlation (All Variables)
    combined_cols = [
        'number_of_persons_injured', 'number_of_persons_killed',
        'temperature', 'humidity', 'wind_speed', 'visibility',
        'has_event', 'weather_severity_score', 'severity_score'
    ]
    corr3 = correlation_data[combined_cols].corr()
    sns.heatmap(corr3, annot=True, fmt='.2f', cmap='coolwarm', center=0,
               square=True, linewidths=1, cbar_kws={"shrink": 0.8}, ax=axes[2])
    axes[2].set_title('Combined Correlation Matrix', fontweight='bold', fontsize=12)
    
    plt.tight_layout()
    plt.savefig('correlation_heatmaps.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: correlation_heatmaps.png")
    plt.close()
    
    # 2. Geographic Correlation Heatmap (by Borough)
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Ensure has_event column exists
    df_geo = df.copy()
    df_geo['has_event'] = df_geo['event_id'].notna().astype(int)
    
    # Aggregate by borough
    borough_stats = df_geo.groupby('borough').agg({
        'number_of_persons_injured': 'mean',
        'number_of_persons_killed': 'mean',
        'temperature': 'mean',
        'humidity': 'mean',
        'wind_speed': 'mean',
        'visibility': 'mean',
        'has_event': 'mean'
    }).T
    
    sns.heatmap(borough_stats, annot=True, fmt='.2f', cmap='YlOrRd',
               square=False, linewidths=1, cbar_kws={"shrink": 0.8}, ax=ax)
    ax.set_title('Geographic Correlation Heatmap: Average Values by Borough', 
                fontweight='bold', fontsize=14)
    ax.set_xlabel('Borough', fontweight='bold')
    ax.set_ylabel('Metrics', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('geographic_correlation_heatmap.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: geographic_correlation_heatmap.png")
    plt.close()
    
    # 3. Weather-Event Interaction Heatmap
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Ensure has_event column exists
    df_interaction = df.copy()
    df_interaction['has_event'] = df_interaction['event_id'].notna()
    
    # Create cross-tabulation of weather and events
    weather_event_cross = pd.crosstab(
        df_interaction['weather_main'], 
        df_interaction['has_event'],
        values=df_interaction['number_of_persons_injured'],
        aggfunc='mean'
    )
    weather_event_cross.columns = ['No Event', 'Has Event']
    
    sns.heatmap(weather_event_cross, annot=True, fmt='.2f', cmap='viridis',
               square=False, linewidths=1, cbar_kws={"shrink": 0.8}, ax=ax)
    ax.set_title('Weather-Event Interaction: Average Injuries', 
                fontweight='bold', fontsize=14)
    ax.set_xlabel('Event Status', fontweight='bold')
    ax.set_ylabel('Weather Type', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('weather_event_interaction_heatmap.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: weather_event_interaction_heatmap.png")
    plt.close()

def create_combined_overlay_map(df):
    """Create a map with all three layers overlaid and detailed information markers"""
    print("Creating combined overlay map...")
    
    m = folium.Map(location=NYC_CENTER, zoom_start=11, tiles='OpenStreetMap')
    
    # Add collision heatmap
    collision_data = []
    for idx, row in df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            weight = row['severity_score'] if pd.notna(row['severity_score']) else 1
            collision_data.append([row['latitude'], row['longitude'], weight * 0.3])  # Reduced opacity
    
    HeatMap(collision_data, min_opacity=0.1, radius=12, blur=8,
           gradient={0.2: 'blue', 0.5: 'cyan', 0.8: 'yellow', 1.0: 'red'},
           name='Collisions Heatmap').add_to(m)
    
    # Add weather heatmap
    weather_data = []
    for idx, row in df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            weather_weight = row['weather_severity_score'] if pd.notna(row['weather_severity_score']) else 1
            visibility_factor = 1 / (row['visibility'] / 10000) if pd.notna(row['visibility']) else 1
            weight = weather_weight * visibility_factor * 0.3
            weather_data.append([row['latitude'], row['longitude'], weight])
    
    HeatMap(weather_data, min_opacity=0.1, radius=15, blur=10,
           gradient={0.2: 'lightblue', 0.5: 'cyan', 0.8: 'orange', 1.0: 'darkred'},
           name='Weather Heatmap').add_to(m)
    
    # Add events heatmap
    events_df = df[df['event_id'].notna()]
    event_data = []
    for idx, row in events_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            event_weight = 2 if 'Sport' in str(row['event_type']) else 1
            if pd.notna(row['street_closure_type']) and row['street_closure_type'] != 'N/A':
                event_weight += 1
            event_data.append([row['latitude'], row['longitude'], event_weight * 0.3])
    
    if len(event_data) > 0:
        HeatMap(event_data, min_opacity=0.1, radius=18, blur=10,
               gradient={0.2: 'green', 0.5: 'lime', 0.8: 'yellow', 1.0: 'red'},
               name='Events Heatmap').add_to(m)
    
    # Create feature groups for detailed markers
    info_markers = folium.FeatureGroup(name='📍 Location Details')
    
    # Group data by rounded coordinates (to aggregate nearby points)
    df['lat_rounded'] = df['latitude'].round(3)
    df['lon_rounded'] = df['longitude'].round(3)
    
    # Aggregate data by location
    location_groups = df.groupby(['lat_rounded', 'lon_rounded']).agg({
        'collision_id': 'count',
        'number_of_persons_injured': 'sum',
        'number_of_persons_killed': 'sum',
        'weather_main': lambda x: ', '.join(x.unique().astype(str)),
        'weather_description': lambda x: ', '.join(x.unique().astype(str))[:100],  # Limit length
        'temperature': 'mean',
        'humidity': 'mean',
        'wind_speed': 'mean',
        'visibility': 'mean',
        'event_id': lambda x: x.notna().sum(),  # Count of events
        'event_name': lambda x: ', '.join(x.dropna().unique().astype(str))[:150] if x.notna().any() else 'None',
        'event_type': lambda x: ', '.join(x.dropna().unique().astype(str)) if x.notna().any() else 'None',
        'latitude': 'first',
        'longitude': 'first',
        'borough': 'first'
    }).reset_index()
    
    # Add markers with detailed information
    for idx, row in location_groups.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Count collisions
            collision_count = int(row['collision_id'])
            total_injured = int(row['number_of_persons_injured']) if pd.notna(row['number_of_persons_injured']) else 0
            total_killed = int(row['number_of_persons_killed']) if pd.notna(row['number_of_persons_killed']) else 0
            event_count = int(row['event_id']) if pd.notna(row['event_id']) else 0
            
            # Build popup content
            popup_html = f"""
            <div style="font-family: Arial, sans-serif; width: 300px;">
                <h3 style="margin: 5px 0; color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
                    📍 Location Details
                </h3>
                
                <div style="background-color: #ecf0f1; padding: 10px; border-radius: 5px; margin: 10px 0;">
                    <h4 style="margin: 5px 0; color: #34495e;">📍 Coordinates</h4>
                    <p style="margin: 3px 0;"><strong>Latitude:</strong> {row['latitude']:.5f}</p>
                    <p style="margin: 3px 0;"><strong>Longitude:</strong> {row['longitude']:.5f}</p>
                    <p style="margin: 3px 0;"><strong>Borough:</strong> {row['borough'] if pd.notna(row['borough']) else 'N/A'}</p>
                </div>
                
                <div style="background-color: #fee; padding: 10px; border-radius: 5px; margin: 10px 0; border-left: 4px solid #e74c3c;">
                    <h4 style="margin: 5px 0; color: #c0392b;">🚗 Collisions</h4>
                    <p style="margin: 3px 0;"><strong>Total Collisions:</strong> <span style="color: #e74c3c; font-size: 18px; font-weight: bold;">{collision_count}</span></p>
                    <p style="margin: 3px 0;"><strong>Total Injured:</strong> {total_injured}</p>
                    <p style="margin: 3px 0;"><strong>Total Killed:</strong> {total_killed}</p>
                </div>
                
                <div style="background-color: #e8f4f8; padding: 10px; border-radius: 5px; margin: 10px 0; border-left: 4px solid #3498db;">
                    <h4 style="margin: 5px 0; color: #2980b9;">🌤️ Weather</h4>
                    <p style="margin: 3px 0;"><strong>Condition:</strong> {row['weather_main'] if pd.notna(row['weather_main']) else 'N/A'}</p>
                    <p style="margin: 3px 0;"><strong>Description:</strong> {str(row['weather_description'])[:50] if pd.notna(row['weather_description']) else 'N/A'}</p>
                    <p style="margin: 3px 0;"><strong>Temperature:</strong> {row['temperature']:.1f}°C</p>
                    <p style="margin: 3px 0;"><strong>Humidity:</strong> {row['humidity']:.0f}%</p>
                    <p style="margin: 3px 0;"><strong>Wind Speed:</strong> {row['wind_speed']:.1f} m/s</p>
                    <p style="margin: 3px 0;"><strong>Visibility:</strong> {row['visibility']:.0f}m</p>
                </div>
                
                <div style="background-color: #eafaf1; padding: 10px; border-radius: 5px; margin: 10px 0; border-left: 4px solid #27ae60;">
                    <h4 style="margin: 5px 0; color: #229954;">🎉 Events</h4>
                    <p style="margin: 3px 0;"><strong>Total Events:</strong> <span style="color: #27ae60; font-size: 18px; font-weight: bold;">{event_count}</span></p>
                    <p style="margin: 3px 0;"><strong>Event Types:</strong> {str(row['event_type'])[:80] if pd.notna(row['event_type']) and str(row['event_type']) != 'None' else 'None'}</p>
                    <p style="margin: 3px 0;"><strong>Event Names:</strong> {str(row['event_name'])[:80] if pd.notna(row['event_name']) and str(row['event_name']) != 'None' else 'None'}</p>
                </div>
            </div>
            """
            
            # Determine marker color based on collision count
            if collision_count >= 3:
                marker_color = 'red'
            elif collision_count >= 2:
                marker_color = 'orange'
            else:
                marker_color = 'blue'
            
            # Create marker
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=8 + (collision_count * 2),  # Size based on collision count
                popup=folium.Popup(popup_html, max_width=350),
                tooltip=f"Collisions: {collision_count} | Events: {event_count} | Weather: {row['weather_main'] if pd.notna(row['weather_main']) else 'N/A'}",
                color=marker_color,
                fill=True,
                fillColor=marker_color,
                fillOpacity=0.6,
                weight=2
            ).add_to(info_markers)
    
    info_markers.add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Add title with legend
    title_html = '''
    <div style="position: fixed; 
                top: 10px; left: 50%; transform: translateX(-50%); 
                z-index:9999; width: 600px; height: auto; 
                background-color: white; padding: 15px;
                border: 2px solid #3498db; border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
        <h3 style="margin: 0 0 10px 0; text-align: center; color: #2c3e50;">
            🗺️ NYC Combined Heatmap: Collisions + Weather + Events
        </h3>
        <p style="margin: 5px 0; text-align: center; color: #7f8c8d; font-size: 12px;">
            Toggle layers using the control panel (top right) | Click markers for detailed information
        </p>
        <div style="display: flex; justify-content: space-around; margin-top: 10px; font-size: 11px;">
            <span>🔵 Blue: 1 collision</span>
            <span>🟠 Orange: 2 collisions</span>
            <span>🔴 Red: 3+ collisions</span>
        </div>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    m.save('nyc_combined_overlay_map.html')
    print("✓ Saved: nyc_combined_overlay_map.html")
    return m

def main():
    """Main function to generate all map visualizations"""
    print("="*80)
    print("NYC MAP VISUALIZATIONS: Collisions, Weather, Events & Correlations")
    print("="*80)
    
    print("\nLoading dataset...")
    df = load_dataset('mock_joined_dataset.json')
    
    print("Preprocessing data...")
    df = preprocess_data(df)
    
    print(f"\nDataset loaded: {len(df)} records")
    print(f"Records with valid coordinates: {df[df['latitude'].notna()].shape[0]}")
    print(f"Records with events: {df['event_id'].notna().sum()}")
    
    print("\n" + "-"*80)
    print("Generating map visualizations...")
    print("-"*80)
    
    # Create individual heatmaps
    create_collision_heatmap(df)
    create_weather_heatmap(df)
    create_events_heatmap(df)
    
    # Create combined overlay
    create_combined_overlay_map(df)
    
    # Create correlation heatmaps
    create_correlation_heatmaps(df)
    
    print("\n" + "="*80)
    print("All visualizations generated successfully!")
    print("="*80)
    print("\nGenerated files:")
    print("  📍 nyc_collisions_heatmap.html - Interactive collision heatmap")
    print("  🌤️  nyc_weather_heatmap.html - Interactive weather/climate heatmap")
    print("  🎉 nyc_events_heatmap.html - Interactive events heatmap")
    print("  🗺️  nyc_combined_overlay_map.html - All layers combined (toggleable)")
    print("  📊 correlation_heatmaps.png - Statistical correlation heatmaps")
    print("  📊 geographic_correlation_heatmap.png - Borough-level correlations")
    print("  📊 weather_event_interaction_heatmap.png - Weather-Event interactions")
    print("\n💡 Tip: Open the HTML files in a web browser for interactive exploration!")

if __name__ == "__main__":
    main()

