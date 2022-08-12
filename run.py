import os
import subprocess

file_path = os.path.dirname(__file__)

os.chdir(file_path)

subprocess.check_output([
    'dbt', 
    'run', 
    '--project-dir', '/home/son/dev/dbt_project/jaffle_shop', 
    '--profiles-dir', '/home/son/.dbt'
    ],
    stderr=subprocess.STDOUT
)
