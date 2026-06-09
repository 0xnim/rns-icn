#!/usr/bin/env python3
"""
Reticulum Network Stack Daemon with embedded ICN support.

This is a modified rnsd that can optionally run an embedded ICN server
sharing the same transport instance.
"""

import RNS
import argparse
import sys
import os
import signal
import asyncio

from RNS._version import __version__

# Try to import ICN modules
try:
    from rns_icn.rns_server import RNSICNServer
    ICN_AVAILABLE = True
except ImportError:
    ICN_AVAILABLE = False


# ICN server instance (global for signal handling)
icn_server = None
icn_loop = None


async def run_daemon(configdir, verbosity=0, quietness=0, service=False, enable_icn=False, icn_identity_path=None):
    """Main async daemon runner."""
    global icn_server, icn_loop
    
    targetverbosity = verbosity - quietness
    
    if service:
        targetlogdest = RNS.LOG_FILE
        targetverbosity = None
    else:
        targetlogdest = RNS.LOG_STDOUT

    reticulum = RNS.Reticulum(configdir=configdir, verbosity=targetverbosity, logdest=targetlogdest)

    if reticulum.is_connected_to_shared_instance:
        RNS.log(
            "Started rnsd version {version} connected to another shared local instance, this is probably NOT what you want!".format(version=__version__),
            RNS.LOG_WARNING
        )
    else:
        RNS.log("Started rnsd version {version}".format(version=__version__), RNS.LOG_NOTICE)

    # Initialize ICN server if enabled
    if enable_icn and ICN_AVAILABLE:
        try:
            if icn_identity_path is None:
                identity_path = os.path.expanduser("~/.icn/identity")
            else:
                identity_path = icn_identity_path

            print(f"[ICN] Identity file: {identity_path}")

            icn_server = RNSICNServer(identity_path=identity_path, app_name="icn", aspect="default")
            icn_server.start()

            print()
            print(f"  Identity hex hash : {icn_server.identity.hexhash}")
            print(f"  Identity hash (16B): {icn_server.identity.hash.hex()}")
            print(f"  Destination hexhash: {icn_server.hexhash}")
            print(f"  Listening on       : /icn/default")
            print()
            print("  Set RNS_DEST on clients to:")
            print(f"    export RNS_DEST={icn_server.hexhash}")
            print()
            print("[ICN] ICN server started and announced")

        except Exception as e:
            RNS.log(f"[ICN] Failed to start ICN server: {e}", RNS.LOG_ERROR)

    # Keep running
    print("Press Ctrl+C to stop.")
    
    # Wait forever
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass


async def shutdown():
    """Graceful shutdown."""
    global icn_server
    print("\n[ICN] Shutting down...")
    if icn_server:
        icn_server.stop()
    # Give time for cleanup
    await asyncio.sleep(0.5)


def main():
    try:
        parser = argparse.ArgumentParser(description="Reticulum Network Stack Daemon with optional ICN")
        parser.add_argument("--config", action="store", default=None, help="path to alternative Reticulum config directory", type=str)
        parser.add_argument('-v', '--verbose', action='count', default=0)
        parser.add_argument('-q', '--quiet', action='count', default=0)
        parser.add_argument('-s', '--service', action='store_true', default=False, help="rnsd is running as a service and should log to file")
        parser.add_argument('-i', '--interactive', action='store_true', default=False, help="drop into interactive shell after initialisation")
        parser.add_argument("--exampleconfig", action='store_true', default=False, help="print verbose configuration example to stdout and exit")
        parser.add_argument("--version", action="version", version="rnsd-icn {version}".format(version=__version__))

        # ICN arguments
        parser.add_argument("--enable-icn", action="store_true", default=False, help="Enable embedded ICN server")
        parser.add_argument("--icn-identity", action="store", default=None, help="Path to ICN identity file (default: ~/.icn/identity)")

        args = parser.parse_args()

        if args.exampleconfig:
            print(__example_rns_config__)
            exit()

        if args.config:
            configarg = args.config
        else:
            configarg = None

        # Setup signal handlers
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def signal_handler():
            loop.create_task(shutdown())
            loop.call_later(1.0, loop.stop)

        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

        if args.interactive:
            # For interactive, just run the async function and then drop to shell
            loop.run_until_complete(run_daemon(
                configdir=configarg,
                verbosity=args.verbose,
                quietness=args.quiet,
                service=args.service,
                enable_icn=args.enable_icn,
                icn_identity_path=args.icn_identity
            ))
            import code
            code.interact(local=globals())
        else:
            loop.run_until_complete(run_daemon(
                configdir=configarg,
                verbosity=args.verbose,
                quietness=args.quiet,
                service=args.service,
                enable_icn=args.enable_icn,
                icn_identity_path=args.icn_identity
            ))

    except KeyboardInterrupt:
        print("")
        exit()


# Re-export the example config from the original rnsd
from RNS.Utilities.rnsd import __example_rns_config__

if __name__ == "__main__":
    main()