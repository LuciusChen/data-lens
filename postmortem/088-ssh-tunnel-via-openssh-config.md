#+title: SSH Tunnels via OpenSSH Config
#+date: 2026-04-24

* Context

clutch needed a first-party SSH tunnel path for databases that are only
reachable through a bastion host.  The requirement was to keep the user-facing
workflow close to normal Emacs and OpenSSH habits instead of inventing a new
credential system inside clutch.

The core design question was where to put the SSH responsibility:

- TRAMP-style remote transport integration
- protocol-library changes in =mysql= / =pg=
- clutch-managed local forwards using the existing =ssh= client

* Decision

Implement SSH tunneling in =clutch= itself, using the user's normal OpenSSH
configuration.

- connection profiles gain =:ssh-host=
- =:ssh-host= names a host alias from =~/.ssh/config=
- clutch starts =ssh -N -L ...= before opening the database connection
- the database client still sees a normal TCP socket, just on a local forwarded
  port
- the UI continues to show the remote database endpoint, not =127.0.0.1:PORT=

* Why We Did Not Depend on TRAMP

TRAMP solves remote file and process access.  clutch only needed a local port
forward with clear start/stop ownership around a database connection.

Depending on TRAMP would have expanded the abstraction in the wrong direction:

- more moving parts than a local forward needs
- weaker control over tunnel lifecycle
- tighter coupling between database connect/disconnect and remote file semantics

For clutch, =make-process= and a small amount of connection-scoped state are the
better fit.

* Why We Reused OpenSSH Instead of Adding SSH Password Handling

OpenSSH already owns the hard parts:

- =~/.ssh/config=
- host aliases
- =ProxyJump=
- =ssh-agent=
- known_hosts and host key policy

Re-implementing password prompts, keyboard-interactive auth, or key passphrase
flows inside clutch would have created a fragile second SSH client.  The first
version therefore assumes the user's normal OpenSSH setup is already working and
keeps clutch focused on tunnel lifecycle.

* Follow-up: Interactive Preparation Without Owning Credentials

The first failure mode users hit was not tunnel lifecycle.  It was OpenSSH
state that is intentionally interactive on first use:

- encrypted private keys that need a passphrase
- keys not yet loaded into =ssh-agent=
- first-time host-key confirmation

We kept the actual =clutch-connect= tunnel in batch mode.  Letting the primary
connection command prompt would make it easy for Emacs to block on an invisible
SSH question, especially when OpenSSH wants a tty or askpass program.  Instead,
we added =M-x clutch-prepare-ssh-host= and the =S= transient entry as an
explicit preparation step.  That command opens a normal interactive OpenSSH
session for the configured host alias; the next =clutch-connect= still uses the
same non-interactive tunnel path as before.

This deliberately does not replace OpenSSH credential management.  If
interactive =ssh HOST= works but =ssh -o BatchMode=yes HOST= does not, the user
still needs to configure the platform's normal non-interactive auth path, such
as =ssh-agent= or =AddKeysToAgent=.  clutch should diagnose that clearly, not
store SSH key passphrases or implement a second SSH client.

* Tradeoff

The first version only supports structured =:host= / =:port= connection params.
Raw JDBC =:url= entries are left out because clutch cannot safely rewrite an
arbitrary URL without taking on driver-specific parsing rules.

That limitation is deliberate.  A smaller, explicit first version is easier to
trust than a broad SSH feature that guesses wrong about connection strings.
