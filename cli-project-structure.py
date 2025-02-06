import os
import click

def create_project_structure(project_name, base_path="."):
    """Creates a structured directory for a data science project."""
    project_path = os.path.join(base_path, project_name)
    directories = [
        "data/raw", "data/processed", "data/external",
        "notebooks", "scripts", "models", "reports",
        "configs", "logs", "tests"
    ]
    
    files = {
        "README.md": "# Project: " + project_name,
        "requirements.txt": "# List dependencies here",
        ".gitignore": "*.pyc\n__pycache__/\n.env\n"
    }
    
    # Create directories
    for directory in directories:
        os.makedirs(os.path.join(project_path, directory), exist_ok=True)
    
    # Create essential files
    for file_name, content in files.items():
        with open(os.path.join(project_path, file_name), "w") as file:
            file.write(content)
    
    click.echo(f"Project '{project_name}' structure created successfully at {project_path}")

@click.command()
@click.argument("project_name")
@click.option("--path", default=".", help="Base path where the project should be created.")
def main(project_name, path):
    """CLI tool to set up a Data Science project structure."""
    create_project_structure(project_name, path)

if __name__ == "__main__":
    main()
