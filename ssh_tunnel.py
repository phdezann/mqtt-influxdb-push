import subprocess


class SSHTunnelException(Exception):
    pass


class SSHTunnel:
    def __init__(self, args):
        self.process = None
        self.args = args

    def start_tunnel(self):
        hostname = self.args.influxdb_hostname
        port = self.args.influxdb_port
        command = ['ssh', '-L', f"{port}:localhost:{port}", hostname]
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        self.wait_for_welcome_msg()

    def wait_for_welcome_msg(self):
        timer = 0
        while timer < 10:
            timer += 1
            line = self.process.stdout.readline()
            if b"Debian GNU/Linux comes with" in line:
                return
        raise SSHTunnelException()
