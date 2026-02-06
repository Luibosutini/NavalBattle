import boto3
import json
from collections import defaultdict

REGION = "ap-northeast-1"
BUCKET = "fleet-tokyo-artifacts-1a9c4e"
TASK_ID = "T-0123"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)

def list_results():
    prefix = f"results/{TASK_ID}/"
    paginator = s3.get_paginator('list_objects_v2')
    
    results = defaultdict(list)
    
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.md'):
                # Extract ship class from filename
                parts = key.split('/')[-1].split('.')
                if len(parts) >= 2:
                    ship_class = parts[1].split('-')[0]
                    results[ship_class].append({
                        'key': key,
                        'size': obj['Size'],
                        'modified': obj['LastModified']
                    })
    
    return results

def get_budget_status():
    try:
        resp = ddb.get_item(
            TableName='fleet-budget',
            Key={'month': {'S': '2026-02'}}
        )
        item = resp.get('Item', {})
        return {
            'fuel_cap': float(item.get('fuel_cap_usd', {}).get('N', 0)),
            'fuel_remaining': float(item.get('fuel_remaining_usd', {}).get('N', 0)),
            'fuel_burned': float(item.get('fuel_burned_usd', {}).get('N', 0)),
            'sorties_total': int(item.get('sorties_total', {}).get('N', 0)),
            'ammo_used': {
                'BB': int(item.get('ammo_used', {}).get('M', {}).get('BB_main_gun', {}).get('N', 0)),
                'CA': int(item.get('ammo_used', {}).get('M', {}).get('CA_salvo', {}).get('N', 0)),
                'CVB': int(item.get('ammo_used', {}).get('M', {}).get('CVB_air_wing', {}).get('N', 0)),
            },
            'ammo_caps': {
                'BB': int(item.get('ammo_caps', {}).get('M', {}).get('BB_main_gun', {}).get('N', 0)),
                'CA': int(item.get('ammo_caps', {}).get('M', {}).get('CA_salvo', {}).get('N', 0)),
                'CVB': int(item.get('ammo_caps', {}).get('M', {}).get('CVB_air_wing', {}).get('N', 0)),
            }
        }
    except Exception as e:
        return {'error': str(e)}

def main():
    print(f"# Fleet Test Report: {TASK_ID}")
    print()
    
    # Budget Status
    print("## Budget Status (2026-02)")
    budget = get_budget_status()
    if 'error' not in budget:
        print(f"- Fuel: ${budget['fuel_burned']:.2f} / ${budget['fuel_cap']:.2f} (${budget['fuel_remaining']:.2f} remaining)")
        print(f"- Total Sorties: {budget['sorties_total']}")
        print(f"- Ammo Usage:")
        print(f"  - BB Main Gun: {budget['ammo_used']['BB']} / {budget['ammo_caps']['BB']}")
        print(f"  - CA Salvo: {budget['ammo_used']['CA']} / {budget['ammo_caps']['CA']}")
        print(f"  - CVB Air Wing: {budget['ammo_used']['CVB']} / {budget['ammo_caps']['CVB']}")
    else:
        print(f"Error: {budget['error']}")
    print()
    
    # Results by Ship Class
    print("## Mission Results by Ship Class")
    results = list_results()
    
    for ship_class in ['CVL', 'DD', 'CL', 'CVB', 'CA', 'BB']:
        missions = results.get(ship_class, [])
        if missions:
            print(f"\n### {ship_class} ({len(missions)} missions)")
            for m in sorted(missions, key=lambda x: x['modified'], reverse=True)[:5]:
                print(f"- {m['key'].split('/')[-1]} ({m['size']} bytes) - {m['modified']}")
    
    print()
    print("## Summary")
    total = sum(len(v) for v in results.values())
    print(f"- Total missions: {total}")
    for ship_class, missions in sorted(results.items()):
        print(f"- {ship_class}: {len(missions)}")

if __name__ == "__main__":
    main()
