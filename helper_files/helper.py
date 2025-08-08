# helper.py

import os

def get_project_root():
    """
    Finds and returns the absolute path of the project's root directory.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    while True:
        if os.path.isdir(os.path.join(current_dir, '.git')):
            return current_dir
        
        parent_dir = os.path.dirname(current_dir)
        
        if parent_dir == current_dir:
            return None
            
        current_dir = parent_dir


def affix_root_path(current_path):
    """
    Takes a path to a dir or file, and affixes the project root before it.
    """
    return os.path.join(get_project_root(), current_path)

