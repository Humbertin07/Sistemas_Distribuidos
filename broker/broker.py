import zmq

def main():
    context = zmq.Context()
    
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")
    
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")
    
    print("[BROKER] Iniciado nas portas 5555 e 5556")
    
    zmq.proxy(frontend, backend)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[BROKER] Encerrando...")