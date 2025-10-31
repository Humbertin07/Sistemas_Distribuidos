import zmq

print("Iniciando Broker (Req-Rep)...")

context = zmq.Context()
frontend = context.socket(zmq.ROUTER)
backend = context.socket(zmq.DEALER)
frontend.bind("tcp://*:5555")
backend.bind("tcp://*:5556")

print("Broker escutando em 5555 (clientes) e 5556 (servidores).")

zmq.proxy(frontend, backend)