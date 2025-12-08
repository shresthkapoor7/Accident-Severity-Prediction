import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

def load_dataset(filename='mock_joined_dataset.json'):
    """Load the mock dataset from JSON file"""
    with open(filename, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def preprocess_data(df):
    """Preprocess the dataframe for analysis"""
    # Convert numeric columns
    numeric_cols = ['number_of_persons_injured', 'number_of_persons_killed',
                    'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
                    'number_of_cyclist_injured', 'number_of_cyclist_killed',
                    'number_of_motorist_injured', 'number_of_motorist_killed',
                    'temperature', 'feels_like', 'humidity', 'pressure',
                    'wind_speed', 'wind_deg', 'clouds', 'visibility']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert date columns
    df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
    df['crash_hour'] = df['crash_time'].str.split(':').str[0].astype(int)
    
    # Create binary flags
    df['has_event'] = df['event_id'].notna()
    df['has_injuries'] = df['number_of_persons_injured'] > 0
    df['has_fatalities'] = df['number_of_persons_killed'] > 0
    df['total_casualties'] = df['number_of_persons_injured'] + df['number_of_persons_killed']
    
    # Categorize weather severity
    df['weather_severity'] = df['weather_main'].map({
        'Clear': 'Good',
        'Clouds': 'Moderate',
        'Rain': 'Poor',
        'Snow': 'Poor',
        'Mist': 'Poor',
        'Fog': 'Poor'
    })
    
    return df

def plot_weather_collision_correlation(df):
    """Plot correlation between weather conditions and collisions"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Weather Conditions vs Collisions Analysis', fontsize=16, fontweight='bold')
    
    # 1. Collisions by weather type
    ax1 = axes[0, 0]
    weather_counts = df['weather_main'].value_counts()
    colors = sns.color_palette("husl", len(weather_counts))
    ax1.bar(weather_counts.index, weather_counts.values, color=colors)
    ax1.set_title('Number of Collisions by Weather Type', fontweight='bold')
    ax1.set_xlabel('Weather Type')
    ax1.set_ylabel('Number of Collisions')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. Average injuries by weather type
    ax2 = axes[0, 1]
    weather_injuries = df.groupby('weather_main')['number_of_persons_injured'].mean()
    ax2.bar(weather_injuries.index, weather_injuries.values, color=colors[:len(weather_injuries)])
    ax2.set_title('Average Injuries per Collision by Weather Type', fontweight='bold')
    ax2.set_xlabel('Weather Type')
    ax2.set_ylabel('Average Injuries')
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. Temperature vs Collisions
    ax3 = axes[1, 0]
    ax3.scatter(df['temperature'], df['number_of_persons_injured'], 
               alpha=0.6, s=50, c=df['total_casualties'], cmap='Reds')
    ax3.set_title('Temperature vs Injuries (Color = Total Casualties)', fontweight='bold')
    ax3.set_xlabel('Temperature (°C)')
    ax3.set_ylabel('Number of Injuries')
    ax3.grid(True, alpha=0.3)
    
    # 4. Visibility vs Collisions
    ax4 = axes[1, 1]
    visibility_injuries = df.groupby('visibility')['number_of_persons_injured'].mean()
    ax4.bar(visibility_injuries.index.astype(str), visibility_injuries.values, 
           color=['#ff6b6b' if v < 10000 else '#51cf66' for v in visibility_injuries.index])
    ax4.set_title('Average Injuries by Visibility Level', fontweight='bold')
    ax4.set_xlabel('Visibility (meters)')
    ax4.set_ylabel('Average Injuries')
    
    plt.tight_layout()
    plt.savefig('weather_collision_correlation.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: weather_collision_correlation.png")
    plt.close()

def plot_event_collision_correlation(df):
    """Plot correlation between events and collisions"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Events vs Collisions Analysis', fontsize=16, fontweight='bold')
    
    # 1. Collisions with vs without events
    ax1 = axes[0, 0]
    event_comparison = df.groupby('has_event').agg({
        'collision_id': 'count',
        'number_of_persons_injured': 'mean',
        'number_of_persons_killed': 'mean'
    })
    x = np.arange(len(event_comparison.index))
    width = 0.25
    ax1.bar(x - width, event_comparison['collision_id'], width, label='Total Collisions', color='#4dabf7')
    ax1.bar(x, event_comparison['number_of_persons_injured'] * 10, width, label='Avg Injuries (×10)', color='#ff8787')
    ax1.bar(x + width, event_comparison['number_of_persons_killed'] * 20, width, label='Avg Fatalities (×20)', color='#ff6b6b')
    ax1.set_xlabel('Has Event')
    ax1.set_ylabel('Count / Average')
    ax1.set_title('Collisions: With vs Without Events', fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(['No Event', 'Has Event'])
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # 2. Collisions by event type
    ax2 = axes[0, 1]
    event_type_counts = df[df['has_event']]['event_type'].value_counts()
    if len(event_type_counts) > 0:
        colors = sns.color_palette("Set2", len(event_type_counts))
        ax2.pie(event_type_counts.values, labels=event_type_counts.index, autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax2.set_title('Distribution of Collisions with Events by Event Type', fontweight='bold')
    else:
        ax2.text(0.5, 0.5, 'No events in dataset', ha='center', va='center')
        ax2.set_title('Distribution of Collisions with Events by Event Type', fontweight='bold')
    
    # 3. Street closure type impact
    ax3 = axes[1, 0]
    closure_data = df[df['street_closure_type'].notna()].groupby('street_closure_type').agg({
        'number_of_persons_injured': 'mean',
        'collision_id': 'count'
    })
    if len(closure_data) > 0:
        x = np.arange(len(closure_data.index))
        ax3_twin = ax3.twinx()
        bars1 = ax3.bar(x - 0.2, closure_data['collision_id'], 0.4, label='Collision Count', color='#4dabf7')
        bars2 = ax3_twin.bar(x + 0.2, closure_data['number_of_persons_injured'], 0.4, 
                            label='Avg Injuries', color='#ff8787')
        ax3.set_xlabel('Street Closure Type')
        ax3.set_ylabel('Collision Count', color='#4dabf7')
        ax3_twin.set_ylabel('Average Injuries', color='#ff8787')
        ax3.set_title('Impact of Street Closure Type on Collisions', fontweight='bold')
        ax3.set_xticks(x)
        ax3.set_xticklabels(closure_data.index, rotation=45, ha='right')
        ax3.tick_params(axis='y', labelcolor='#4dabf7')
        ax3_twin.tick_params(axis='y', labelcolor='#ff8787')
        ax3.legend(loc='upper left')
        ax3_twin.legend(loc='upper right')
    else:
        ax3.text(0.5, 0.5, 'No street closure data', ha='center', va='center')
        ax3.set_title('Impact of Street Closure Type on Collisions', fontweight='bold')
    
    # 4. Event agency vs collisions
    ax4 = axes[1, 1]
    agency_data = df[df['event_agency'].notna()]['event_agency'].value_counts()
    if len(agency_data) > 0:
        colors = sns.color_palette("viridis", len(agency_data))
        ax4.barh(agency_data.index, agency_data.values, color=colors)
        ax4.set_title('Collisions by Event Agency', fontweight='bold')
        ax4.set_xlabel('Number of Collisions')
        ax4.set_ylabel('Event Agency')
    else:
        ax4.text(0.5, 0.5, 'No event agency data', ha='center', va='center')
        ax4.set_title('Collisions by Event Agency', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('event_collision_correlation.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: event_collision_correlation.png")
    plt.close()

def plot_combined_analysis(df):
    """Plot combined analysis of weather, events, and collisions"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Combined Analysis: Weather + Events + Collisions', fontsize=16, fontweight='bold')
    
    # 1. Weather severity vs Event presence
    ax1 = axes[0, 0]
    weather_event = pd.crosstab(df['weather_severity'], df['has_event'], normalize='index') * 100
    weather_event.plot(kind='bar', ax=ax1, color=['#ff6b6b', '#51cf66'], width=0.8)
    ax1.set_title('Event Presence by Weather Severity', fontweight='bold')
    ax1.set_xlabel('Weather Severity')
    ax1.set_ylabel('Percentage (%)')
    ax1.legend(['No Event', 'Has Event'])
    ax1.tick_params(axis='x', rotation=0)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # 2. Weather + Event impact on injuries
    ax2 = axes[0, 1]
    df['weather_event_combo'] = df.apply(
        lambda x: f"{x['weather_severity']} + {'Event' if x['has_event'] else 'No Event'}", axis=1
    )
    combo_injuries = df.groupby('weather_event_combo')['number_of_persons_injured'].mean().sort_values(ascending=False)
    colors = sns.color_palette("RdYlGn_r", len(combo_injuries))
    ax2.barh(combo_injuries.index, combo_injuries.values, color=colors)
    ax2.set_title('Average Injuries: Weather + Event Combination', fontweight='bold')
    ax2.set_xlabel('Average Injuries')
    ax2.set_ylabel('Weather + Event Status')
    
    # 3. Hourly pattern with weather
    ax3 = axes[1, 0]
    hourly_weather = df.groupby(['crash_hour', 'weather_severity'])['collision_id'].count().unstack(fill_value=0)
    hourly_weather.plot(kind='line', ax=ax3, marker='o', linewidth=2)
    ax3.set_title('Hourly Collision Pattern by Weather Severity', fontweight='bold')
    ax3.set_xlabel('Hour of Day')
    ax3.set_ylabel('Number of Collisions')
    ax3.legend(title='Weather Severity')
    ax3.grid(True, alpha=0.3)
    ax3.set_xticks(range(0, 24, 2))
    
    # 4. Correlation heatmap
    ax4 = axes[1, 1]
    correlation_cols = ['number_of_persons_injured', 'number_of_persons_killed',
                       'temperature', 'humidity', 'wind_speed', 'visibility',
                       'has_event', 'clouds', 'pressure']
    corr_data = df[correlation_cols].corr()
    sns.heatmap(corr_data, annot=True, fmt='.2f', cmap='coolwarm', center=0,
               square=True, linewidths=1, cbar_kws={"shrink": 0.8}, ax=ax4)
    ax4.set_title('Correlation Matrix: Key Variables', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('combined_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: combined_analysis.png")
    plt.close()

def plot_geographic_analysis(df):
    """Plot geographic distribution of collisions"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Geographic Analysis: Collisions by Borough', fontsize=16, fontweight='bold')
    
    # 1. Collisions by borough
    ax1 = axes[0, 0]
    borough_counts = df['borough'].value_counts()
    colors = sns.color_palette("Set3", len(borough_counts))
    ax1.bar(borough_counts.index, borough_counts.values, color=colors)
    ax1.set_title('Total Collisions by Borough', fontweight='bold')
    ax1.set_xlabel('Borough')
    ax1.set_ylabel('Number of Collisions')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. Average injuries by borough
    ax2 = axes[0, 1]
    borough_injuries = df.groupby('borough')['number_of_persons_injured'].mean().sort_values(ascending=False)
    ax2.barh(borough_injuries.index, borough_injuries.values, 
            color=colors[:len(borough_injuries)])
    ax2.set_title('Average Injuries per Collision by Borough', fontweight='bold')
    ax2.set_xlabel('Average Injuries')
    ax2.set_ylabel('Borough')
    
    # 3. Events by borough
    ax3 = axes[1, 0]
    borough_events = df[df['has_event']]['borough'].value_counts()
    if len(borough_events) > 0:
        ax3.pie(borough_events.values, labels=borough_events.index, autopct='%1.1f%%',
               colors=colors[:len(borough_events)], startangle=90)
        ax3.set_title('Event Distribution by Borough', fontweight='bold')
    else:
        ax3.text(0.5, 0.5, 'No events in dataset', ha='center', va='center')
        ax3.set_title('Event Distribution by Borough', fontweight='bold')
    
    # 4. Weather by borough
    ax4 = axes[1, 1]
    borough_weather = pd.crosstab(df['borough'], df['weather_main'], normalize='index') * 100
    borough_weather.plot(kind='bar', ax=ax4, stacked=True, colormap='Set2', width=0.8)
    ax4.set_title('Weather Distribution by Borough', fontweight='bold')
    ax4.set_xlabel('Borough')
    ax4.set_ylabel('Percentage (%)')
    ax4.legend(title='Weather Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('geographic_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: geographic_analysis.png")
    plt.close()

def plot_temporal_analysis(df):
    """Plot temporal patterns"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Temporal Analysis: Time Patterns in Collisions', fontsize=16, fontweight='bold')
    
    # 1. Collisions by hour
    ax1 = axes[0, 0]
    hourly_counts = df.groupby('crash_hour')['collision_id'].count()
    ax1.plot(hourly_counts.index, hourly_counts.values, marker='o', linewidth=2, markersize=8)
    ax1.fill_between(hourly_counts.index, hourly_counts.values, alpha=0.3)
    ax1.set_title('Collisions by Hour of Day', fontweight='bold')
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Number of Collisions')
    ax1.set_xticks(range(0, 24, 2))
    ax1.grid(True, alpha=0.3)
    
    # 2. Collisions by day of week
    ax2 = axes[0, 1]
    df['day_of_week'] = df['crash_date'].dt.day_name()
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_counts = df['day_of_week'].value_counts().reindex(day_order, fill_value=0)
    colors = sns.color_palette("husl", len(day_counts))
    ax2.bar(day_counts.index, day_counts.values, color=colors)
    ax2.set_title('Collisions by Day of Week', fontweight='bold')
    ax2.set_xlabel('Day of Week')
    ax2.set_ylabel('Number of Collisions')
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. Injuries by hour
    ax3 = axes[1, 0]
    hourly_injuries = df.groupby('crash_hour')['number_of_persons_injured'].mean()
    ax3.bar(hourly_injuries.index, hourly_injuries.values, color='#ff6b6b', alpha=0.7)
    ax3.set_title('Average Injuries by Hour of Day', fontweight='bold')
    ax3.set_xlabel('Hour of Day')
    ax3.set_ylabel('Average Injuries')
    ax3.set_xticks(range(0, 24, 2))
    ax3.grid(True, alpha=0.3, axis='y')
    
    # 4. Collisions over time (by date)
    ax4 = axes[1, 1]
    daily_counts = df.groupby(df['crash_date'].dt.date)['collision_id'].count()
    ax4.plot(daily_counts.index, daily_counts.values, marker='o', linewidth=2, markersize=4)
    ax4.set_title('Daily Collision Count Over Time', fontweight='bold')
    ax4.set_xlabel('Date')
    ax4.set_ylabel('Number of Collisions')
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('temporal_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: temporal_analysis.png")
    plt.close()

def generate_summary_statistics(df):
    """Generate and print summary statistics"""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    print(f"\nTotal Collisions: {len(df)}")
    print(f"Collisions with Events: {df['has_event'].sum()} ({df['has_event'].mean()*100:.1f}%)")
    print(f"Total Injuries: {df['number_of_persons_injured'].sum()}")
    print(f"Total Fatalities: {df['number_of_persons_killed'].sum()}")
    
    print("\n--- Weather Distribution ---")
    print(df['weather_main'].value_counts())
    
    print("\n--- Average Injuries by Weather ---")
    print(df.groupby('weather_main')['number_of_persons_injured'].mean().sort_values(ascending=False))
    
    print("\n--- Collisions with Events vs Without Events ---")
    event_comparison = df.groupby('has_event').agg({
        'number_of_persons_injured': ['mean', 'sum'],
        'number_of_persons_killed': ['mean', 'sum']
    })
    print(event_comparison)
    
    print("\n--- Correlation Coefficients ---")
    corr_cols = ['number_of_persons_injured', 'temperature', 'humidity', 
                'wind_speed', 'visibility', 'has_event']
    corr_matrix = df[corr_cols].corr()['number_of_persons_injured'].sort_values(ascending=False)
    print(corr_matrix)
    
    print("\n" + "="*80)

def main():
    """Main function to run all visualizations"""
    print("Loading dataset...")
    df = load_dataset('mock_joined_dataset.json')
    
    print("Preprocessing data...")
    df = preprocess_data(df)
    
    print("\nGenerating visualizations...")
    print("-" * 80)
    
    # Generate all visualizations
    plot_weather_collision_correlation(df)
    plot_event_collision_correlation(df)
    plot_combined_analysis(df)
    plot_geographic_analysis(df)
    plot_temporal_analysis(df)
    
    # Print summary statistics
    generate_summary_statistics(df)
    
    print("\n" + "="*80)
    print("All visualizations generated successfully!")
    print("="*80)
    print("\nGenerated files:")
    print("  - weather_collision_correlation.png")
    print("  - event_collision_correlation.png")
    print("  - combined_analysis.png")
    print("  - geographic_analysis.png")
    print("  - temporal_analysis.png")

if __name__ == "__main__":
    main()

