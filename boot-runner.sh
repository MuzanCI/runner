#!/bin/sh

# Exit if a command exits with non-zero status.
set -e

# Treat unset variables as an error and exit.
set -u

echo "Starting boot-runner.sh..."

# ------------------------------
# Prechecks
# ------------------------------

# Ensure the script is being run as root
if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root."
  exit 1
fi

# ------------------------------
# Staff user configuration
# ------------------------------

pw useradd admin -m -c "System administrator" -G wheel -s /usr/local/bin/bash -h -
pw user show admin

cat << EOF > /usr/local/etc/doas.conf
permit nopass admin as root
EOF

# ------------------------------
# SSHD hardening
# ------------------------------

SSHD_CONFIG="/etc/ssh/sshd_config"

cat << 'EOF' > "$SSHD_CONFIG"
PermitRootLogin no
PasswordAuthentication no
ChallengeResponseAuthentication no
X11Forwarding no
EOF

service sshd stop
service sshd disable

# ------------------------------
# Firewall configuration
# ------------------------------

PF_CONFIG="/etc/pf.conf"

cat << EOF > "$PF_CONFIG"
# =====================================================================
# VARIABLES
# =====================================================================

# Internal interface, connected to the private network
vlan_if = "vtnet0"
vlan_subnet = "10.0.0.0/16"

# =====================================================================
# GLOBAL OPTIONS
# =====================================================================

# Block packets by dropping them.
set block-policy drop

# To prevent unnecessary filtering on local packets, skip the loopback interface.
set skip on lo0

# To prevent fragmentation attacks, normalize and reassemble packets.
scrub in all

# =====================================================================
# DEFAULT FILTERS
# =====================================================================

# By default, block all incoming traffic and log it for monitoring.
block in log all

# Allow all outgoing traffic and keep state for return packets.
pass out all keep state

# =====================================================================
# INTERNAL vLAN INTERFACE RULES
# =====================================================================

# Allow DHCP replies from the DHCP server
pass in quick on $vlan_interface proto udp from any port 67 to any port 68

# Allow full communication between verified vLAN members
pass in quick on $vlan_interface proto { tcp, udp, icmp } from $vlan_subnet to any keep state
EOF

chmod 0600 "$PF_CONFIG"

sysrc pflog_logfile="/var/log/pflog"
service pflog enable
service pflog start

sysrc pf_rules="$PF_CONFIG"
service pf enable
service pf start

# ------------------------------
# Internal network configuration
# ------------------------------

# Configure DNS nameservers
cat << EOF > /etc/resolv.conf
search muzanci.local
nameserver 10.0.0.1
nameserver 1.1.1.1
EOF

# Configure DHCP client.
cat << EOF > /etc/dhclient.conf
supersede domain-name "muzanci.local";
supersede domain-name-servers 10.0.0.1, 1.1.1.1;
EOF

# Configure the vtnet0 interface with DHCP and bring it up
sysrc ifconfig_vtnet0="DHCP"
dhclient vtnet0

# ------------------------------
# Package installation
# ------------------------------

pkg update
pkg upgrade

pkg install -y doas
pkg install -y bash
pkg install -y vim

# ------------------------------
# Tailscale
# ------------------------------

pkg install -y tailscale

sysrc tailscaled_up_args="--ssh --accept-dns=false --auth-key=tskey-auth-kZ9j8vY9sG11CNTRL-Z3iLeMBidzXZtgbRkpvD1YmhLGcTuhLj"

service tailscaled enable
service tailscaled start

echo "boot-runner.sh completed successfully."
