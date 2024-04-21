import socket

def check_connection(host, port):
    try:
        socket.create_connection((host, port), timeout=5)
        return True
    except OSError:
        return False

if __name__ == "__main__":
    host = "spark-master"
    port = 7077
    if check_connection(host, port):
        print(f"Connection to {host}:{port} is successful.")
    else:
        print(f"Failed to connect to {host}:{port}.")
