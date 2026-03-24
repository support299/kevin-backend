import os
import django
import json
import datetime

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db import connection

def serialize_dt(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def check_tables():
    results = {}
    with connection.cursor() as cursor:
        # Check opportunity_report columns
        cursor.execute("SELECT * FROM opportunity_report LIMIT 1")
        row = cursor.fetchone()
        colnames = [desc[0] for desc in cursor.description]
        results['opportunity_report'] = {
            'columns': colnames,
            'has_custom_fields': 'custom_fields' in colnames,
        }
        if row:
            results['opportunity_report']['sample_row'] = list(row)

        # Check contact_report columns
        cursor.execute("SELECT * FROM contact_report LIMIT 1")
        row = cursor.fetchone()
        colnames = [desc[0] for desc in cursor.description]
        results['contact_report'] = {
            'columns': colnames,
            'has_custom_fields': 'custom_fields' in colnames,
        }
        if row:
            results['contact_report']['sample_row'] = list(row)
            if 'custom_fields' in colnames:
                cf_idx = colnames.index('custom_fields')
                results['contact_report']['custom_fields_sample'] = row[cf_idx]

    with open('db_results.json', 'w') as f:
        json.dump(results, f, default=serialize_dt, indent=2)

if __name__ == "__main__":
    check_tables()
