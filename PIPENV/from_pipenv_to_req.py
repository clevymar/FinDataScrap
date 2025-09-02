import toml
import json

pipfile_path = "Pipfile"
pipfile_lock_path = "Pipfile.lock"
requirements_path = "requirements.txt"

# Read Pipfile
with open(pipfile_path, "r") as f:
    pipfile = toml.load(f)

# Get all package names
packages = list(pipfile.get("packages", {}).keys())
dev_packages = list(pipfile.get("dev-packages", {}).keys())
all_packages = packages + dev_packages

# Read Pipfile.lock
with open(pipfile_lock_path, "r") as f:
    pipfile_lock = json.load(f)

# Get versions for each package
lines = []
for pkg in all_packages:
    version = None
    for section in ["default", "develop"]:
        if pkg in pipfile_lock.get(section, {}):
            version = pipfile_lock[section][pkg].get("version")
            break
    if version:
        lines.append(f"{pkg}{version}")
    else:
        lines.append(pkg)  # fallback if version not found

# Write to requirements.txt
with open(requirements_path, "w") as f:
    f.write("\n".join(lines))

print("requirements.txt created.")