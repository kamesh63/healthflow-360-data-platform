import os

dbt_dir = os.path.join(os.path.expanduser('~'), '.dbt')
os.makedirs(dbt_dir, exist_ok=True)

password = "YOUR_ACTUAL_PASSWORD"  # Replace this

content = (
    "healthflow:\n"
    "  target: dev\n"
    "  outputs:\n"
    "    dev:\n"
    "      type: snowflake\n"
    "      account: YOUR_SNOWFLAKE_ACCOUNT\n"
    "      user: user\n"
    "      password: " + "mypassword" + "\n" 
    "      role: ACCOUNTADMIN\n"
    "      warehouse: HEALTHFLOW_WH\n"
    "      database: HEALTHFLOW_DB\n"
    "      schema: DBT_DEV\n"
    "      threads: 4\n"
    "      client_session_keep_alive: False\n"
)

path = os.path.join(dbt_dir, 'profiles.yml')
with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Created:", path)
print("Content preview:")
print(content)