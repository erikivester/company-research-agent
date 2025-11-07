import os
import sys
import py_compile
from pathlib import Path

def check_syntax(file_path):
    try:
        py_compile.compile(file_path, doraise=True)
        return None
    except Exception as e:
        return str(e)

def main():
    base_path = Path(__file__).parent
    errors = []
    
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.py') and file != 'syntax_check.py':
                file_path = os.path.join(root, file)
                error = check_syntax(file_path)
                if error:
                    rel_path = os.path.relpath(file_path, base_path)
                    errors.append(f"{rel_path}: {error}")
    
    if errors:
        print("\nSyntax errors found:")
        for error in errors:
            print(f"\n{error}")
        sys.exit(1)
    else:
        print("\nNo syntax errors found in Python files.")
        sys.exit(0)

if __name__ == '__main__':
    main()