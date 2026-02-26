#!/bin/bash

# Check for unpinned dependencies in pyproject.toml
set -e

check_pyproject_toml() {
  local filename="$1"

  if [[ ! -f "$filename" ]]; then
    echo "ERROR: File not found: $filename"
    return 1
  fi

  # Extract dependencies from pyproject.toml and check if they're pinned
  local unpinned_deps=()
  local in_dependencies=false
  local in_dev_dependencies=false
  local bracket_count=0

  while IFS= read -r line; do
    # Check if we're entering dependencies section
    if [[ "$line" =~ ^dependencies[[:space:]]*=[[:space:]]*\[ ]]; then
      in_dependencies=true
      bracket_count=1
      continue
    fi

    # Check if we're entering dev dependencies section
    if [[ "$line" =~ ^dev[[:space:]]*=[[:space:]]*\[ ]]; then
      in_dev_dependencies=true
      bracket_count=1
      continue
    fi

    # Track bracket depth
    if [[ "$in_dependencies" == true ]] || [[ "$in_dev_dependencies" == true ]]; then
      [[ "$line" =~ \[ ]] && ((bracket_count++)) || true
      [[ "$line" =~ \] ]] && ((bracket_count--)) || true

      # Exit section when brackets close
      if [[ $bracket_count -eq 0 ]]; then
        in_dependencies=false
        in_dev_dependencies=false
        continue
      fi

      # Extract dependency string
      if [[ "$line" =~ \"([^\"]+)\" ]]; then
        local dep="${BASH_REMATCH[1]}"
        # Check if dependency has exact version pinning (==)
        if [[ ! "$dep" =~ == ]]; then
          unpinned_deps+=("$dep")
        fi
      fi
    fi
  done < "$filename"

  if [[ ${#unpinned_deps[@]} -gt 0 ]]; then
    echo "ERROR: Unpinned dependencies found in $filename:"
    printf '%s\n' "${unpinned_deps[@]}"
    return 1
  fi

  return 0
}

# Check pyproject.toml
check_pyproject_toml "pyproject.toml"

echo "All dependencies are properly pinned."
exit 0
