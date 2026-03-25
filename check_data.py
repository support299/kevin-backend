import os
import django
import json

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db import connection

def check_field_data():
    results = {}
    with connection.cursor() as cursor:
        # Check opportunity_report columns
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'opportunity_report'")
        cf_cols = [row[0] for row in cursor.fetchall() if row[0].startswith('cf_')]
        
        if cf_cols:
            # Build query to find rows where at least one cf_ column is not null
            # Using COALESCE to check if any of these columns has a value
            # This can be heavy on a 700k table, so we'll be careful
            # Let's just check the last 1000 records
            where_clause = " OR ".join([f'"{col}" IS NOT NULL AND "{col}" <> \'\'' for col in cf_cols[:10]]) # checking first 10 for speed
            sql = f'SELECT id, opportunity_id, {", ".join([f'"{col}"' for col in cf_cols])} FROM opportunity_report WHERE {where_clause} LIMIT 3'
            
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                if rows:
                    results['opportunity_fields_with_data'] = [list(r) for r in rows]
                else:
                    results['opportunity_fields_with_data'] = "No rows found with data in the first 10 cf_ columns yet."
            except Exception as e:
                results['opportunity_fields_with_data'] = f"Error: {e}"
        else:
            results['opportunity_fields_with_data'] = "No cf_ columns found."

        # Same for contact_report
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'contact_report'")
        cf_cols = [row[0] for row in cursor.fetchall() if row[0].startswith('cf_')]
        
        if cf_cols:
            # Check first 50 columns
            where_clause = " OR ".join([f'"{col}" IS NOT NULL AND "{col}" <> \'\'' for col in cf_cols[:5]])
            sql = f'SELECT id, {", ".join([f'"{col}"' for col in cf_cols[:5]])} FROM contact_report WHERE {where_clause} LIMIT 3'
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                if rows:
                    results['contact_fields_with_data'] = [list(r) for r in rows]
                else:
                    results['contact_fields_with_data'] = "No rows found with data in selected cf_ columns yet."
            except Exception as e:
                results['contact_fields_with_data'] = f"Error: {e}"
        else:
            results['contact_fields_with_data'] = "No cf_ columns found."

    with open('db_data_check.json', 'w') as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    check_field_data()
