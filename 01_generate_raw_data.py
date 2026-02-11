"""
Generate sample data for Time Doctor analytics demo
Author: Max
Date: Feb 2025

Creates 6 months of fake time tracking data - accounts, users, apps, sessions etc.
Trying to make it realistic enough for the dashboard
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import random
import os

# config
RAW_DIR = 'data/raw'
START = datetime(2025, 8, 1)
END = datetime(2026, 1, 31)

# make it reproducible
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

os.makedirs(RAW_DIR, exist_ok=True)

print("\n" + "="*60)
print("Generating sample data for Time Doctor dashboard")
print("="*60 + "\n")

# ACCOUNTS - just 5 fake companies
print("Creating accounts...")
accounts_df = pd.DataFrame({
    'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'],
    'company_name': [
        'TechStart Inc',
        'Global Services LLC', 
        'Innovation Labs',
        'Digital Solutions Corp',
        'Creative Agency Group'
    ],
    'industry': ['Technology', 'Consulting', 'Software', 'IT Services', 'Marketing'],
    'company_size': ['Small', 'Medium', 'Large', 'Medium', 'Small'],
    'account_status': ['active', 'active', 'active', 'active', 'active'],
    'subscription_tier': ['pro', 'enterprise', 'pro', 'business', 'business'],
    'account_created_date': ['2024-01-15', '2023-06-20', '2024-03-10', '2023-11-05', '2024-05-22'],
    'primary_contact': [fake.email() for _ in range(5)],
    'timezone': ['America/New_York', 'America/Chicago', 'America/Los_Angeles', 'America/New_York', 'America/Denver']
})
accounts_df.to_csv(f'{RAW_DIR}/dim_accounts.csv', index=False)
print(f"  -> {len(accounts_df)} accounts")

# USERS - 50 fake employees
print("Creating users...")
users = []
for i in range(1, 51):
    acct = f"ACC{str((i % 5) + 1).zfill(3)}"
    fname = fake.first_name()
    lname = fake.last_name()
    
    users.append({
        'user_id': f'USR{str(i).zfill(4)}',
        'account_id': acct,
        'email': f"{fname.lower()}.{lname.lower()}@company.com",
        'first_name': fname,
        'last_name': lname,
        'role': random.choice(['Developer', 'Senior Developer', 'Designer', 'Product Manager', 'QA Engineer']),
        'department': random.choice(['Engineering', 'Product', 'Marketing', 'Operations']),
        'manager_id': f"USR{str(random.randint(1, 10)).zfill(4)}" if i > 5 else None,
        'hire_date': fake.date_between(start_date='-3y', end_date='-1m'),
        'status': 'active',
        'timezone': random.choice(['America/New_York', 'America/Los_Angeles']),
        'weekly_capacity_hours': 40
    })

users_df = pd.DataFrame(users)
users_df.to_csv(f'{RAW_DIR}/dim_users.csv', index=False)
print(f"  -> {len(users_df)} users")

# APPS - the usual suspects
print("Creating applications...")
apps = [
    # dev tools
    {'app_id': 'APP001', 'app_name': 'Visual Studio Code', 'category': 'Development', 'productivity_classification': 'productive'},
    {'app_id': 'APP002', 'app_name': 'IntelliJ IDEA', 'category': 'Development', 'productivity_classification': 'productive'},
    {'app_id': 'APP003', 'app_name': 'PyCharm', 'category': 'Development', 'productivity_classification': 'productive'},
    
    # project mgmt
    {'app_id': 'APP005', 'app_name': 'Jira', 'category': 'Productivity', 'productivity_classification': 'productive'},
    {'app_id': 'APP006', 'app_name': 'Trello', 'category': 'Productivity', 'productivity_classification': 'productive'},
    {'app_id': 'APP008', 'app_name': 'Notion', 'category': 'Productivity', 'productivity_classification': 'productive'},
    
    # design
    {'app_id': 'APP009', 'app_name': 'Figma', 'category': 'Design', 'productivity_classification': 'productive'},
    {'app_id': 'APP010', 'app_name': 'Adobe Photoshop', 'category': 'Design', 'productivity_classification': 'productive'},
    
    # communication - neutral
    {'app_id': 'APP012', 'app_name': 'Slack', 'category': 'Communication', 'productivity_classification': 'neutral'},
    {'app_id': 'APP013', 'app_name': 'Microsoft Teams', 'category': 'Communication', 'productivity_classification': 'neutral'},
    {'app_id': 'APP014', 'app_name': 'Zoom', 'category': 'Communication', 'productivity_classification': 'neutral'},
    
    # browsers
    {'app_id': 'APP016', 'app_name': 'Google Chrome', 'category': 'Browsing', 'productivity_classification': 'neutral'},
    {'app_id': 'APP017', 'app_name': 'Firefox', 'category': 'Browsing', 'productivity_classification': 'neutral'},
    
    # email
    {'app_id': 'APP019', 'app_name': 'Gmail', 'category': 'Email', 'productivity_classification': 'neutral'},
    {'app_id': 'APP020', 'app_name': 'Outlook', 'category': 'Email', 'productivity_classification': 'neutral'},
    
    # docs
    {'app_id': 'APP021', 'app_name': 'Google Docs', 'category': 'Documentation', 'productivity_classification': 'productive'},
    {'app_id': 'APP022', 'app_name': 'Microsoft Word', 'category': 'Documentation', 'productivity_classification': 'productive'},
    
    # version control
    {'app_id': 'APP024', 'app_name': 'GitHub Desktop', 'category': 'Development', 'productivity_classification': 'productive'},
    
    # time wasters
    {'app_id': 'APP026', 'app_name': 'YouTube', 'category': 'Entertainment', 'productivity_classification': 'unproductive'},
    {'app_id': 'APP027', 'app_name': 'Netflix', 'category': 'Entertainment', 'productivity_classification': 'unproductive'},
    {'app_id': 'APP028', 'app_name': 'Facebook', 'category': 'Social Media', 'productivity_classification': 'unproductive'},
]

apps_df = pd.DataFrame(apps)
apps_df['is_web_based'] = apps_df['app_name'].apply(lambda x: 'Google' in x or 'Gmail' in x or x in ['Jira', 'Figma'])
apps_df['version'] = [f"{np.random.randint(1,5)}.{np.random.randint(0,10)}" for _ in range(len(apps_df))]
apps_df.to_csv(f'{RAW_DIR}/dim_applications.csv', index=False)
print(f"  -> {len(apps_df)} applications")

# PROJECTS
print("Creating projects...")
project_list = [
    'Website Redesign', 'Mobile App v2.0', 'API Integration', 'Database Migration',
    'Customer Portal', 'Analytics Dashboard', 'Marketing Campaign Q4', 'Security Audit',
    'Performance Optimization', 'User Onboarding Flow', 'Payment Gateway',
    'Content Management System', 'Email Automation', 'Inventory System',
    'Reporting Module', 'Search Feature', 'Admin Panel', 'Chat Integration',
    'Data Pipeline', 'ML Model Training'
]

projects = []
for i, name in enumerate(project_list[:20], 1):
    projects.append({
        'project_id': f'PRJ{str(i).zfill(3)}',
        'account_id': f"ACC{str((i % 5) + 1).zfill(3)}",
        'project_name': name,
        'status': random.choice(['active', 'active', 'active', 'on_hold']),
        'start_date': fake.date_between(start_date='-6m', end_date='-1m'),
        'end_date': fake.date_between(start_date='today', end_date='+6m') if random.random() > 0.3 else None,
        'budgeted_hours': random.randint(50, 500),
        'priority': random.choice(['High', 'Medium', 'Low'])
    })

projects_df = pd.DataFrame(projects)
projects_df.to_csv(f'{RAW_DIR}/dim_projects.csv', index=False)
print(f"  -> {len(projects_df)} projects")

# TASKS
print("Creating tasks...")
tasks = []
task_types = ['Bug Fix', 'Feature Development', 'Code Review', 'Testing', 'Documentation', 'Meeting']
task_id = 1
for proj in projects_df['project_id']:
    for _ in range(random.randint(3, 8)):
        tasks.append({
            'task_id': f'TSK{str(task_id).zfill(5)}',
            'project_id': proj,
            'task_name': random.choice(task_types),
            'task_status': random.choice(['todo', 'in_progress', 'completed', 'blocked']),
            'estimated_hours': random.randint(1, 20),
            'created_date': fake.date_between(start_date='-3m', end_date='today')
        })
        task_id += 1

tasks_df = pd.DataFrame(tasks)
tasks_df.to_csv(f'{RAW_DIR}/dim_tasks.csv', index=False)
print(f"  -> {len(tasks_df)} tasks")

# SESSIONS - this takes a while
print("Creating activity sessions (this takes ~30 sec)...")
sessions = []
session_id = 1
current = START

# target spread at latest date:
#   Highly Active:     ~20 users (40%) - active 5-7 days/week
#   Moderately Active: ~15 users (30%) - active 3-4 days/week
#   Low Activity:      ~10 users (20%) - active 1-2 days/week
#   Inactive:          ~5 users  (10%) - churned, 0 active days recently
user_activity_type = {}
for u in users_df['user_id']:
    u_num = int(u.replace('USR', ''))
    if u_num in [5, 15, 25, 35, 45]:   # exactly 5 churned users (10%)
        user_activity_type[u] = 'churned'
    elif u_num % 5 == 1:                # 10 low activity users (20%)
        user_activity_type[u] = 'low'
    elif u_num % 5 == 2:                # 10 low activity users (20%)
        user_activity_type[u] = 'low'
    elif u_num % 5 == 3:                # 10 moderate users
        user_activity_type[u] = 'moderate'
    elif u_num % 5 == 4:                # 10 moderate users (30% total moderate)
        user_activity_type[u] = 'moderate'
    else:                               # 15 highly active users (30%)
        user_activity_type[u] = 'high'

# stagger user cohort start dates across 6 months
# this creates 6 distinct cohort weeks in the retention heatmap
def get_cohort_start(user_id):
    u_num = int(user_id.replace('USR', ''))
    if u_num <= 8:    return datetime(2025, 8,  1)
    elif u_num <= 16: return datetime(2025, 9,  1)
    elif u_num <= 24: return datetime(2025, 10, 1)
    elif u_num <= 32: return datetime(2025, 11, 1)
    elif u_num <= 40: return datetime(2025, 12, 1)
    else:             return datetime(2026, 1,  1)

user_cohort_start = {u: get_cohort_start(u) for u in users_df['user_id']}
print("  -> Cohort distribution:")
for label, lo, hi in [('Aug 2025', 1, 8), ('Sep 2025', 9, 16), ('Oct 2025', 17, 24),
                       ('Nov 2025', 25, 32), ('Dec 2025', 33, 40), ('Jan 2026', 41, 50)]:
    print(f"     {label}: users {lo}-{hi}")

# spread churn dates so they don't all go inactive at once
user_churn_date = {}
churn_users = [u for u, t in user_activity_type.items() if t == 'churned']
churn_days = [45, 60, 80, 110, 140]
for u, days in zip(sorted(churn_users), churn_days):
    user_churn_date[u] = START + timedelta(days=days)

while current <= END:
    # skip most weekends
    if current.weekday() >= 5 and random.random() < 0.7:
        current += timedelta(days=1)
        continue
    
    # each user creates some sessions
    for user in users_df['user_id']:
        activity = user_activity_type[user]

        # only generate sessions from user's cohort start date
        # this is what creates distinct cohort weeks in the retention table
        if current < user_cohort_start[user]:
            continue

        # churned users stop tracking after their churn date
        if activity == 'churned':
            if current > user_churn_date[user]:
                continue
        
        # skip days based on activity type
        # high:     active ~90% of weekdays  (6-7 days/week) -> Highly Active tier
        # moderate: active ~55% of weekdays  (3-4 days/week) -> Moderately Active tier
        # low:      active ~25% of weekdays  (1-2 days/week) -> Low Activity tier
        # churned:  active until churn date, then nothing    -> Inactive tier
        if activity == 'high' and random.random() < 0.10:
            continue
        elif activity == 'moderate' and random.random() < 0.45:
            continue
        elif activity == 'low' and random.random() < 0.75:
            continue
        
        num_sessions = random.randint(2, 8)
        
        for _ in range(num_sessions):
            # pick an app (weighted toward productive)
            app_weights = []
            for app in apps:
                if app['productivity_classification'] == 'productive':
                    app_weights.append(0.6)
                elif app['productivity_classification'] == 'neutral':
                    app_weights.append(0.3)
                else:
                    app_weights.append(0.1)
            
            app = random.choices(apps, weights=app_weights)[0]
            
            # session length - most are 15-60 min
            duration = max(5, min(180, np.random.lognormal(3, 0.8)))
            
            # start time during work hours
            hour = random.randint(8, 17)
            minute = random.randint(0, 59)
            start_time = current.replace(hour=hour, minute=minute)
            
            # sometimes has a project
            has_project = random.random() < 0.7
            proj = random.choice(projects_df['project_id']) if has_project else None
            task = random.choice(tasks_df['task_id']) if has_project and random.random() < 0.6 else None
            
            # activity level
            activity = np.random.beta(5, 2) * 100
            
            sessions.append({
                'session_id': f'SES{str(session_id).zfill(8)}',
                'user_id': user,
                'account_id': users_df[users_df['user_id'] == user]['account_id'].values[0],
                'app_id': app['app_id'],
                'project_id': proj,
                'task_id': task,
                'date': current.date(),
                'start_timestamp': start_time,
                'duration_minutes': round(duration, 2),
                'keyboard_strokes': int(duration * random.randint(50, 200)) if activity > 30 else 0,
                'mouse_clicks': int(duration * random.randint(20, 100)) if activity > 30 else 0,
                'activity_percentage': round(activity, 2),
                'is_manual_entry': random.random() < 0.05,
                'screenshot_count': int(duration / 10) if random.random() < 0.8 else 0
            })
            
            session_id += 1
    
    current += timedelta(days=1)
    
    # progress update
    if (current - START).days % 30 == 0:
        pct = ((current - START).days / (END - START).days) * 100
        print(f"  ... {pct:.0f}% done")

sessions_df = pd.DataFrame(sessions)

# add some duplicates to test data quality
dupes = sessions_df.sample(n=int(len(sessions_df) * 0.005))
sessions_df = pd.concat([sessions_df, dupes], ignore_index=True)

sessions_df.to_csv(f'{RAW_DIR}/fact_activity_sessions.csv', index=False)
print(f"  -> {len(sessions_df):,} sessions")

print("\n" + "="*60)
print("Done! Raw data ready in data/raw/")
print("="*60)
print("\nNext: python 02_load_to_sqlite.py\n")
