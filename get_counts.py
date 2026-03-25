import os
import django
import json

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db import connection

def get_counts():
    results = {}
    with connection.cursor() as cursor:
        # Count opportunities per pipeline
        cursor.execute("""
            SELECT 
                pipeline_id, 
                COALESCE(MAX(pipeline_name), pipeline_id) as pipeline_name, 
                COUNT(*) as count
            FROM opportunity_report
            GROUP BY pipeline_id
            ORDER BY count DESC
        """)
        opps_counts = []
        for row in cursor.fetchall():
            opps_counts.append({
                'pipeline_id': row[0],
                'pipeline_name': row[1],
                'count': row[2]
            })
        results['opportunity_counts_by_pipeline'] = opps_counts
        
        # Total opportunities count
        cursor.execute("SELECT COUNT(*) FROM opportunity_report")
        results['total_opportunities'] = cursor.fetchone()[0]

        # Total contacts count
        cursor.execute("SELECT COUNT(*) FROM contact_report")
        results['total_contacts'] = cursor.fetchone()[0]

    with open('db_counts.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary to console
    print(f"\n--- DATABASE SUMMARY ---")
    print(f"Total Contacts: {results['total_contacts']}")
    print(f"Total Opportunities: {results['total_opportunities']}")
    print(f"\nOpportunities by Pipeline:")
    for p in opps_counts:
        print(f" - {p['pipeline_name']} ({p['pipeline_id']}): {p['count']}")
    print(f"------------------------\n")

if __name__ == "__main__":
    get_counts()
