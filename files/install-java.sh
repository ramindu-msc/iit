#!/bin/bash

# Update package lists
sudo apt update

# Install OpenJDK
sudo apt install openjdk-21-jdk -y

# Get the installation path of OpenJDK
JAVA_PATH=$(update-alternatives --query java | grep 'Value: ' | grep -o '/.*/bin/java')
JAVA_HOME=$(update-alternatives --query java | grep 'Value: ' | grep -o '/.*/bin/java' | xargs dirname | xargs dirname)

# Set JAVA_HOME in /etc/profile
echo "export JAVA_HOME=$JAVA_HOME" | sudo tee -a /etc/profile

# Install Maven (Ensuring Maven 3.9.0 or compatible version)
sudo apt install maven -y

# Ensure the installed Maven version is 3.9.0 or later
MAVEN_VERSION=$(mvn -v | head -n 1 | grep -oP '\d+\.\d+\.\d+')

if [[ "$MAVEN_VERSION" < "3.9.0" ]]; then
  echo "Installing Maven 3.9.0 as the installed version is below the required version"

  # Remove the installed Maven
  sudo apt remove maven -y

  # Download and install Maven 3.9.0 manually
  wget https://archive.apache.org/dist/maven/maven-3/3.9.0/binaries/apache-maven-3.9.0-bin.tar.gz -P /tmp
  sudo tar -xvzf /tmp/apache-maven-3.9.0-bin.tar.gz -C /opt
  sudo ln -s /opt/apache-maven-3.9.0/bin/mvn /usr/bin/mvn
fi

# Get the installation path of Maven
MAVEN_HOME=$(mvn -v | grep 'Maven home' | awk '{print $3}')

# Set MAVEN_HOME in /etc/profile
echo "export MAVEN_HOME=$MAVEN_HOME" | sudo tee -a /etc/profile

# Add Maven to PATH in /etc/profile
echo "export PATH=\$PATH:\$MAVEN_HOME/bin" | sudo tee -a /etc/profile

# Load the environment variables
source /etc/profile

# Verify installation and environment variables
java -version
echo "JAVA_HOME is set to: $JAVA_HOME"
mvn -v
echo "MAVEN_HOME is set to: $MAVEN_HOME"
