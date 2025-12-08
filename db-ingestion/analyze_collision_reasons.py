"""
NYC Collision Reasons Analysis
Analyzes all contributing factors and reasons for collisions from MongoDB
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from collections import Counter
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# MongoDB Configuration
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "nyc_collisions"
MONGO_COLLECTION_NAME = "collisions"

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 10

class CollisionReasonAnalyzer:
    """Class to analyze collision reasons from MongoDB"""
    
    def __init__(self, host=MONGO_HOST, port=MONGO_PORT, 
                 db_name=MONGO_DB_NAME, collection_name=MONGO_COLLECTION_NAME):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        self.df = None
    
    def connect(self):
        """Connect to MongoDB"""
        try:
            print(f"Connecting to MongoDB at {self.host}:{self.port}...")
            self.client = MongoClient(self.host, self.port, serverSelectionTimeoutMS=5000)
            self.client.server_info()
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            print(f"✓ Connected to database: {self.db_name}, collection: {self.collection_name}")
            return True
        except Exception as e:
            print(f"✗ Error connecting to MongoDB: {e}")
            return False
    
    def load_data(self, limit=None):
        """Load collision data from MongoDB into DataFrame"""
        print(f"\nLoading collision data from MongoDB...")
        
        try:
            # Build query
            query = {}
            projection = {
                'collision_id': 1,
                'crash_date': 1,
                'crash_time': 1,
                'borough': 1,
                'contributing_factor_vehicle_1': 1,
                'contributing_factor_vehicle_2': 1,
                'contributing_factor_vehicle_3': 1,
                'contributing_factor_vehicle_4': 1,
                'contributing_factor_vehicle_5': 1,
                'number_of_persons_injured': 1,
                'number_of_persons_killed': 1,
                'number_of_pedestrians_injured': 1,
                'number_of_pedestrians_killed': 1,
                'number_of_cyclist_injured': 1,
                'number_of_cyclist_killed': 1,
                'number_of_motorist_injured': 1,
                'number_of_motorist_killed': 1,
                'vehicle_type_code1': 1,
                'vehicle_type_code2': 1,
                'vehicle_type_code3': 1,
                'vehicle_type_code4': 1,
                'vehicle_type_code5': 1,
                'latitude': 1,
                'longitude': 1
            }
            
            cursor = self.collection.find(query, projection)
            if limit:
                cursor = cursor.limit(limit)
            
            data = list(cursor)
            print(f"✓ Loaded {len(data)} records")
            
            # Convert to DataFrame
            self.df = pd.DataFrame(data)
            
            # Convert numeric columns
            numeric_cols = ['number_of_persons_injured', 'number_of_persons_killed',
                          'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
                          'number_of_cyclist_injured', 'number_of_cyclist_killed',
                          'number_of_motorist_injured', 'number_of_motorist_killed',
                          'latitude', 'longitude']
            
            for col in numeric_cols:
                if col in self.df.columns:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
            
            # Convert dates
            if 'crash_date' in self.df.columns:
                self.df['crash_date'] = pd.to_datetime(self.df['crash_date'], errors='coerce')
                self.df['crash_year'] = self.df['crash_date'].dt.year
                self.df['crash_month'] = self.df['crash_date'].dt.month
                self.df['crash_day_of_week'] = self.df['crash_date'].dt.day_name()
            
            # Extract crash hour
            if 'crash_time' in self.df.columns:
                self.df['crash_hour'] = self.df['crash_time'].str.split(':').str[0].astype(int, errors='ignore')
            
            # Create severity score
            self.df['severity_score'] = (
                self.df['number_of_persons_injured'].fillna(0) * 1 +
                self.df['number_of_persons_killed'].fillna(0) * 10 +
                self.df['number_of_pedestrians_injured'].fillna(0) * 1.5 +
                self.df['number_of_pedestrians_killed'].fillna(0) * 15
            )
            
            print(f"✓ Data processed: {len(self.df)} records")
            return True
            
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def extract_all_contributing_factors(self):
        """Extract all contributing factors from all vehicle columns"""
        factors = []
        
        for idx, row in self.df.iterrows():
            # Check all contributing factor columns
            for col in ['contributing_factor_vehicle_1', 'contributing_factor_vehicle_2',
                       'contributing_factor_vehicle_3', 'contributing_factor_vehicle_4',
                       'contributing_factor_vehicle_5']:
                if col in row and pd.notna(row[col]) and str(row[col]).strip() != '':
                    factor = str(row[col]).strip()
                    if factor.lower() not in ['unspecified', 'none', 'null', 'nan']:
                        factors.append({
                            'factor': factor,
                            'collision_id': row.get('collision_id'),
                            'severity_score': row.get('severity_score', 0),
                            'injured': row.get('number_of_persons_injured', 0),
                            'killed': row.get('number_of_persons_killed', 0),
                            'borough': row.get('borough'),
                            'year': row.get('crash_year'),
                            'hour': row.get('crash_hour')
                        })
        
        return pd.DataFrame(factors)
    
    def analyze_contributing_factors(self):
        """Analyze contributing factors"""
        print("\n" + "="*80)
        print("ANALYZING CONTRIBUTING FACTORS")
        print("="*80)
        
        factors_df = self.extract_all_contributing_factors()
        
        if len(factors_df) == 0:
            print("✗ No contributing factors found in data")
            return None
        
        print(f"\nTotal contributing factor instances: {len(factors_df):,}")
        print(f"Unique contributing factors: {factors_df['factor'].nunique()}")
        
        # Top contributing factors by frequency
        factor_counts = factors_df['factor'].value_counts()
        print(f"\n{'='*80}")
        print("TOP 20 CONTRIBUTING FACTORS (by frequency)")
        print(f"{'='*80}")
        for i, (factor, count) in enumerate(factor_counts.head(20).items(), 1):
            pct = (count / len(factors_df)) * 100
            print(f"{i:2d}. {factor:50s} {count:8,} ({pct:5.2f}%)")
        
        # Top contributing factors by severity
        factor_severity = factors_df.groupby('factor').agg({
            'severity_score': ['sum', 'mean', 'count'],
            'injured': 'sum',
            'killed': 'sum'
        }).round(2)
        factor_severity.columns = ['total_severity', 'avg_severity', 'count', 'total_injured', 'total_killed']
        factor_severity = factor_severity.sort_values('total_severity', ascending=False)
        
        print(f"\n{'='*80}")
        print("TOP 20 CONTRIBUTING FACTORS (by total severity)")
        print(f"{'='*80}")
        for i, (factor, row) in enumerate(factor_severity.head(20).iterrows(), 1):
            print(f"{i:2d}. {factor:50s} Severity: {row['total_severity']:,.0f} | "
                  f"Injured: {int(row['total_injured']):,} | Killed: {int(row['total_killed']):,}")
        
        return factors_df, factor_counts, factor_severity
    
    def plot_contributing_factors(self, factors_df, factor_counts):
        """Create visualizations for contributing factors"""
        print("\n" + "="*80)
        print("GENERATING VISUALIZATIONS")
        print("="*80)
        
        # 1. Top contributing factors bar chart
        fig, axes = plt.subplots(2, 2, figsize=(20, 16))
        fig.suptitle('NYC Collision Contributing Factors Analysis', fontsize=18, fontweight='bold')
        
        # Top 20 by frequency
        ax1 = axes[0, 0]
        top_20 = factor_counts.head(20)
        colors = sns.color_palette("husl", len(top_20))
        bars = ax1.barh(range(len(top_20)), top_20.values, color=colors)
        ax1.set_yticks(range(len(top_20)))
        ax1.set_yticklabels([f[:40] + '...' if len(f) > 40 else f for f in top_20.index], fontsize=9)
        ax1.set_xlabel('Number of Occurrences', fontweight='bold')
        ax1.set_title('Top 20 Contributing Factors (by Frequency)', fontweight='bold', fontsize=12)
        ax1.invert_yaxis()
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, (idx, val) in enumerate(top_20.items()):
            ax1.text(val, i, f' {val:,}', va='center', fontsize=8)
        
        # Top 20 by severity
        ax2 = axes[0, 1]
        factor_severity = factors_df.groupby('factor')['severity_score'].sum().sort_values(ascending=False).head(20)
        colors2 = sns.color_palette("RdYlGn_r", len(factor_severity))
        bars2 = ax2.barh(range(len(factor_severity)), factor_severity.values, color=colors2)
        ax2.set_yticks(range(len(factor_severity)))
        ax2.set_yticklabels([f[:40] + '...' if len(f) > 40 else f for f in factor_severity.index], fontsize=9)
        ax2.set_xlabel('Total Severity Score', fontweight='bold')
        ax2.set_title('Top 20 Contributing Factors (by Total Severity)', fontweight='bold', fontsize=12)
        ax2.invert_yaxis()
        ax2.grid(True, alpha=0.3, axis='x')
        
        # Contributing factors by borough
        ax3 = axes[1, 0]
        borough_factors = factors_df.groupby(['borough', 'factor']).size().unstack(fill_value=0)
        top_10_factors = factor_counts.head(10).index
        borough_factors_top = borough_factors[top_10_factors] if all(f in borough_factors.columns for f in top_10_factors) else borough_factors.iloc[:, :10]
        borough_factors_top.plot(kind='bar', stacked=True, ax=ax3, colormap='Set3')
        ax3.set_xlabel('Borough', fontweight='bold')
        ax3.set_ylabel('Number of Occurrences', fontweight='bold')
        ax3.set_title('Top Contributing Factors by Borough', fontweight='bold', fontsize=12)
        ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax3.tick_params(axis='x', rotation=45)
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Contributing factors over time
        ax4 = axes[1, 1]
        if 'year' in factors_df.columns and factors_df['year'].notna().any():
            yearly_factors = factors_df.groupby(['year', 'factor']).size().unstack(fill_value=0)
            top_5_factors = factor_counts.head(5).index
            if all(f in yearly_factors.columns for f in top_5_factors):
                yearly_factors[top_5_factors].plot(kind='line', ax=ax4, marker='o', linewidth=2)
                ax4.set_xlabel('Year', fontweight='bold')
                ax4.set_ylabel('Number of Occurrences', fontweight='bold')
                ax4.set_title('Top 5 Contributing Factors Over Time', fontweight='bold', fontsize=12)
                ax4.legend(title='Contributing Factor', fontsize=8)
                ax4.grid(True, alpha=0.3)
            else:
                ax4.text(0.5, 0.5, 'Insufficient data for time series', 
                        ha='center', va='center', transform=ax4.transAxes)
                ax4.set_title('Top Contributing Factors Over Time', fontweight='bold', fontsize=12)
        else:
            ax4.text(0.5, 0.5, 'No date data available', 
                    ha='center', va='center', transform=ax4.transAxes)
            ax4.set_title('Top Contributing Factors Over Time', fontweight='bold', fontsize=12)
        
        plt.tight_layout()
        plt.savefig('collision_contributing_factors_analysis.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: collision_contributing_factors_analysis.png")
        plt.close()
        
        # 2. Detailed analysis by factor type
        fig, axes = plt.subplots(2, 2, figsize=(20, 16))
        fig.suptitle('Detailed Contributing Factors Analysis', fontsize=18, fontweight='bold')
        
        # Average severity by factor
        ax1 = axes[0, 0]
        avg_severity = factors_df.groupby('factor')['severity_score'].mean().sort_values(ascending=False).head(20)
        colors = sns.color_palette("Reds", len(avg_severity))
        bars = ax1.barh(range(len(avg_severity)), avg_severity.values, color=colors)
        ax1.set_yticks(range(len(avg_severity)))
        ax1.set_yticklabels([f[:40] + '...' if len(f) > 40 else f for f in avg_severity.index], fontsize=9)
        ax1.set_xlabel('Average Severity Score', fontweight='bold')
        ax1.set_title('Top 20 Factors by Average Severity per Incident', fontweight='bold', fontsize=12)
        ax1.invert_yaxis()
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Injuries and fatalities by factor
        ax2 = axes[0, 1]
        factor_stats = factors_df.groupby('factor').agg({
            'injured': 'sum',
            'killed': 'sum'
        }).sort_values('injured', ascending=False).head(15)
        
        x = np.arange(len(factor_stats))
        width = 0.35
        ax2.bar(x - width/2, factor_stats['injured'], width, label='Injured', color='#ff6b6b', alpha=0.8)
        ax2.bar(x + width/2, factor_stats['killed'] * 10, width, label='Killed (×10)', color='#c92a2a', alpha=0.8)
        ax2.set_xlabel('Contributing Factor', fontweight='bold')
        ax2.set_ylabel('Count', fontweight='bold')
        ax2.set_title('Total Injuries and Fatalities by Factor', fontweight='bold', fontsize=12)
        ax2.set_xticks(x)
        ax2.set_xticklabels([f[:20] + '...' if len(f) > 20 else f for f in factor_stats.index], 
                           rotation=45, ha='right', fontsize=8)
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Factors by hour of day
        ax3 = axes[1, 0]
        if 'hour' in factors_df.columns and factors_df['hour'].notna().any():
            hourly_factors = factors_df.groupby(['hour', 'factor']).size().unstack(fill_value=0)
            top_5_factors = factor_counts.head(5).index
            if all(f in hourly_factors.columns for f in top_5_factors):
                hourly_factors[top_5_factors].plot(kind='line', ax=ax3, marker='o', linewidth=2)
                ax3.set_xlabel('Hour of Day', fontweight='bold')
                ax3.set_ylabel('Number of Occurrences', fontweight='bold')
                ax3.set_title('Top 5 Factors by Hour of Day', fontweight='bold', fontsize=12)
                ax3.set_xticks(range(0, 24, 2))
                ax3.legend(title='Contributing Factor', fontsize=8)
                ax3.grid(True, alpha=0.3)
            else:
                ax3.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', transform=ax3.transAxes)
        else:
            ax3.text(0.5, 0.5, 'No hour data available', ha='center', va='center', transform=ax3.transAxes)
        
        # Factor co-occurrence (when multiple factors present)
        ax4 = axes[1, 1]
        # Count collisions with multiple factors
        collision_factor_count = factors_df.groupby('collision_id').size()
        multi_factor_counts = collision_factor_count[collision_factor_count > 1].value_counts().sort_index()
        if len(multi_factor_counts) > 0:
            ax4.bar(multi_factor_counts.index, multi_factor_counts.values, color='#4dabf7', alpha=0.8)
            ax4.set_xlabel('Number of Contributing Factors per Collision', fontweight='bold')
            ax4.set_ylabel('Number of Collisions', fontweight='bold')
            ax4.set_title('Distribution of Multiple Contributing Factors', fontweight='bold', fontsize=12)
            ax4.grid(True, alpha=0.3, axis='y')
        else:
            ax4.text(0.5, 0.5, 'No multi-factor collisions found', 
                    ha='center', va='center', transform=ax4.transAxes)
        
        plt.tight_layout()
        plt.savefig('collision_detailed_analysis.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: collision_detailed_analysis.png")
        plt.close()
    
    def generate_summary_report(self, factors_df, factor_counts, factor_severity):
        """Generate a comprehensive summary report"""
        print("\n" + "="*80)
        print("COMPREHENSIVE SUMMARY REPORT")
        print("="*80)
        
        total_collisions = len(self.df)
        total_factors = len(factors_df)
        unique_factors = factors_df['factor'].nunique()
        
        print(f"\nDataset Overview:")
        print(f"  Total Collisions: {total_collisions:,}")
        print(f"  Total Contributing Factor Instances: {total_factors:,}")
        print(f"  Unique Contributing Factors: {unique_factors}")
        print(f"  Average Factors per Collision: {total_factors/total_collisions:.2f}")
        
        print(f"\nInjury Statistics:")
        total_injured = self.df['number_of_persons_injured'].sum()
        total_killed = self.df['number_of_persons_killed'].sum()
        print(f"  Total Injured: {int(total_injured):,}")
        print(f"  Total Killed: {int(total_killed):,}")
        print(f"  Average Injured per Collision: {total_injured/total_collisions:.2f}")
        
        print(f"\nTop 10 Most Dangerous Factors (by total injuries + fatalities):")
        factor_danger = factors_df.groupby('factor').agg({
            'injured': 'sum',
            'killed': 'sum'
        })
        factor_danger['total_impact'] = factor_danger['injured'] + (factor_danger['killed'] * 10)
        factor_danger = factor_danger.sort_values('total_impact', ascending=False).head(10)
        
        for i, (factor, row) in enumerate(factor_danger.iterrows(), 1):
            print(f"  {i:2d}. {factor:50s} | Injured: {int(row['injured']):6,} | "
                  f"Killed: {int(row['killed']):4,} | Total Impact: {row['total_impact']:,.0f}")
        
        # Save report to file
        with open('collision_analysis_report.txt', 'w') as f:
            f.write("="*80 + "\n")
            f.write("NYC COLLISION CONTRIBUTING FACTORS ANALYSIS REPORT\n")
            f.write("="*80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Dataset Overview:\n")
            f.write(f"  Total Collisions: {total_collisions:,}\n")
            f.write(f"  Total Contributing Factor Instances: {total_factors:,}\n")
            f.write(f"  Unique Contributing Factors: {unique_factors}\n\n")
            f.write("Top 50 Contributing Factors (by frequency):\n")
            for i, (factor, count) in enumerate(factor_counts.head(50).items(), 1):
                pct = (count / total_factors) * 100
                f.write(f"  {i:2d}. {factor:50s} {count:8,} ({pct:5.2f}%)\n")
        
        print(f"\n✓ Report saved to: collision_analysis_report.txt")
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()

def main():
    """Main function"""
    print("="*80)
    print("NYC COLLISION CONTRIBUTING FACTORS ANALYSIS")
    print("="*80)
    
    analyzer = CollisionReasonAnalyzer()
    
    if not analyzer.connect():
        print("✗ Failed to connect to MongoDB. Exiting.")
        return
    
    if not analyzer.load_data():
        print("✗ Failed to load data. Exiting.")
        return
    
    # Analyze contributing factors
    results = analyzer.analyze_contributing_factors()
    if results:
        factors_df, factor_counts, factor_severity = results
        
        # Create visualizations
        analyzer.plot_contributing_factors(factors_df, factor_counts)
        
        # Generate report
        analyzer.generate_summary_report(factors_df, factor_counts, factor_severity)
    
    analyzer.close()
    
    print("\n" + "="*80)
    print("✓ Analysis complete!")
    print("="*80)
    print("\nGenerated files:")
    print("  - collision_contributing_factors_analysis.png")
    print("  - collision_detailed_analysis.png")
    print("  - collision_analysis_report.txt")

if __name__ == "__main__":
    main()

