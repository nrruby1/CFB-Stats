import yaml
from main import app


def export_openapi_yaml():
    # Fetch the generated OpenAPI dictionary
    openapi_schema = app.openapi()

    # Write to a file in YAML format
    with open("Resources/datahandling.yaml", "w") as f:
        yaml.dump(openapi_schema, f, sort_keys=False)


if __name__ == "__main__":
    export_openapi_yaml()
