import zmq

print("Iniciando Proxy (Pub-Sub)...")

context = zmq.Context()
xsub_socket = context.socket(zmq.XSUB)
xpub_socket = context.socket(zmq.XPUB)
xsub_socket.bind("tcp://*:5557")
xpub_socket.bind("tcp://*:5558")

print("Proxy escutando em 5557 (servidores) e 5558 (clientes).")

zmq.proxy(xsub_socket, xpub_socket)