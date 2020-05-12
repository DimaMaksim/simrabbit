import yaml
with open("macrotask.yaml") as file:
    macro=yaml.full_load(file)


print(macro)
