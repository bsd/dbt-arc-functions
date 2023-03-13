import ruamel.yaml


def initialize_yaml():
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=4, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.width = 5000
    return yaml
