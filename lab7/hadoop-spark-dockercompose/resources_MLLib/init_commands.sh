# init_commands.sh
#!/bin/bash
apt-get update && apt-get install -y python3-pip  # Install pip
pip3 install numpy  # Use pip3 to install numpy
exec "$@"  # Preserve the original entrypoint
