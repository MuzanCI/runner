# Runner Host System Requirements

pf rules are assumed to exist for NAT table
dummy net pipes are assumed to exist.
sysrctl properties are assumed to be set.
/boot/loader.conf is assumed to be set.
devfs service is assumed to be started.

Jail IP=11.0.1.$SLOT_ID
Netmask=255.255.0.0
Broadcast=11.0.255.255
Bridge IP=11.0.0.1
