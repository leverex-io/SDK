# simple script to auto update pip deps

import subprocess
import pkg_resources
from pkg_resources import parse_requirements

def get_latest_version(package_name):
    process = subprocess.Popen(['pip', 'install', '{}=='.format(package_name)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    latest_version = ''
    for line in stderr.decode().split('\n'):
        if '(from versions:' in line:
            versions = line.split(':')[-1].strip(' )').split(', ')
            latest_version = versions[-1]
            break

    return latest_version

def update_requirements(file_path):
    with open(file_path, 'r') as file:
        requirements = file.readlines()

    updated_requirements = []
    for req_line in requirements:
        parsed_req = list(parse_requirements(req_line))[0]
        package_name = parsed_req.name
        latest_version = get_latest_version(package_name)
        if latest_version:
            updated_requirements.append('{}=={}\n'.format(package_name, latest_version))
        else:
            updated_requirements.append(req_line)

    with open(file_path, 'w') as file:
        file.writelines(updated_requirements)

update_requirements('requirements.txt')

